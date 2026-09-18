'use strict';

const { now } = require('./util');

let nextConnId = 1;

/**
 * 单条连接的运行时状态。
 * unacked: Map<roomId, Map<seq, {frame, lastSent, tries}>> —— 已推送但未被客户端
 * 累积 ACK 确认的消息，超时重发；这是「至少一次投递」的服务端正，配合客户端
 * 按 seq 去重（幂等消费）达到效果上的恰好一次。
 */
class Connection {
  constructor(ws, user) {
    this.id = nextConnId++;
    this.ws = ws;
    this.userId = user.id;
    this.name = user.name;
    this.connectedAt = now();
    this.lastPong = now(); // 最近一次收到 pong 的时间，心跳判活依据
    this.lastActiveAt = now(); // 最近一次活动（建连/收帧/pong），在线状态展示依据
    this.rooms = new Set(); // 本连接已加入的房间
    this.unacked = new Map();
    this.unackedCount = 0;
  }

  /** 刷新活动时间（收到任意帧 / pong） */
  touch(t = now()) {
    this.lastActiveAt = t;
  }

  trackUnacked(roomId, seq, frame) {
    let room = this.unacked.get(roomId);
    if (!room) {
      room = new Map();
      this.unacked.set(roomId, room);
    }
    room.set(seq, { frame, lastSent: now(), tries: 0 });
    this.unackedCount++;
  }

  /** 累积 ACK：清除 roomId 下所有 seq <= ackSeq 的未确认项，返回新确认的数量 */
  ack(roomId, ackSeq) {
    const room = this.unacked.get(roomId);
    if (!room) return 0;
    let cleared = 0;
    for (const seq of room.keys()) {
      if (seq <= ackSeq) {
        room.delete(seq);
        cleared++;
      }
    }
    if (room.size === 0) this.unacked.delete(roomId);
    this.unackedCount -= cleared;
    return cleared;
  }

  /** 摘出所有超时未确认、需要重发的条目 */
  *pendingResends(staleMs) {
    const t = now();
    for (const room of this.unacked.values()) {
      for (const entry of room.values()) {
        if (t - entry.lastSent >= staleMs) yield entry;
      }
    }
  }
}

/**
 * 连接注册中心：全局/按用户/按房间的连接索引，广播，心跳与重发扫描，
 * 以及房间成员在线状态（presence）管理。
 *
 * Presence 状态机按 (房间, 用户) 维度维护，只取两个对外可见状态：
 *
 *   online  ──最后一条连接离开房间且非主动 leave──▶  grace（宽限倒计时，对外仍表现为在线）
 *     ▲                                                  │ 重连并入房（撤销倒计时，无广播）
 *     └──────────────────────────────────────────────────┘
 *                                                        │ 倒计时到期
 *                                                        ▼
 *                                                     offline（广播后清除条目）
 *
 *   主动 leave：立即 offline（用户主动离开房间不属于断线抖动，不等待宽限）
 *
 * 关键不变量：
 * 1. 多设备聚合 —— 只要该用户在该房间还有任意一条连接，状态始终为 online；
 *    宽限只在「全部连接都离开」时启动，因此多设备不会误报离线。
 * 2. 抖动抑制 —— 宽限窗口内重连只撤销倒计时、不补发 online（对外状态从未翻转，
 *    客户端看不到 online/offline 反复跳变）。
 * 3. 心跳超时与对端关闭殊途同归：terminate() 必然触发 close 事件，统一走 remove()
 *    进入宽限流程，最近活动时间取连接上记录的末次活动。
 */
class Hub {
  constructor(config) {
    this.config = config;
    this.all = new Set(); // 全部连接
    this.byUser = new Map(); // userId -> Set<Connection>
    this.byRoom = new Map(); // roomId -> Set<Connection>
    // roomId -> Map<userId, { name, lastActiveAt, timer }>
    // timer 为 null 表示 online；持有 setTimeout 句柄表示处于离线宽限（grace）中
    this.presence = new Map();
    this.presenceListeners = new Set();
  }

  /** 准入控制：全局上限 + 单用户上限。返回 null 表示可接入，否则返回拒绝原因码。 */
  checkAdmission(userId) {
    if (this.all.size >= this.config.maxConnections) return 'SERVER_FULL';
    const mine = this.byUser.get(userId);
    if (mine && mine.size >= this.config.maxConnectionsPerUser) return 'TOO_MANY_DEVICES';
    return null;
  }

  add(conn) {
    this.all.add(conn);
    let set = this.byUser.get(conn.userId);
    if (!set) {
      set = new Set();
      this.byUser.set(conn.userId, set);
    }
    set.add(conn);
  }

  remove(conn) {
    this.all.delete(conn);
    const mine = this.byUser.get(conn.userId);
    if (mine) {
      mine.delete(conn);
      if (mine.size === 0) this.byUser.delete(conn.userId);
    }
    // 连接关闭（对端断开 / 心跳超时 terminate 后的 close）：逐房间进入离线宽限，
    // 而非立即广播离线 —— 给断线抖动的快速重连留撤销窗口。
    for (const roomId of [...conn.rooms]) {
      this._leaveRoomSet(roomId, conn);
      this._userLeftRoom(conn, roomId, /* immediate */ false);
    }
    conn.rooms.clear();
    conn.unacked.clear();
    conn.unackedCount = 0;
  }

  joinRoom(conn, roomId) {
    this._addRoomSet(roomId, conn);
    conn.rooms.add(roomId);
    this._markOnline(conn, roomId);
  }

  _addRoomSet(roomId, conn) {
    let set = this.byRoom.get(roomId);
    if (!set) {
      set = new Set();
      this.byRoom.set(roomId, set);
    }
    set.add(conn);
  }

  /** 主动离开房间：立即离线（非抖动场景，不等待宽限） */
  leaveRoom(conn, roomId) {
    const wasInRoom = conn.rooms.has(roomId);
    this._leaveRoomSet(roomId, conn);
    conn.rooms.delete(roomId);
    const room = conn.unacked.get(roomId);
    if (room) {
      conn.unackedCount -= room.size;
      conn.unacked.delete(roomId);
    }
    if (wasInRoom) this._userLeftRoom(conn, roomId, /* immediate */ true);
  }

  _leaveRoomSet(roomId, conn) {
    const set = this.byRoom.get(roomId);
    if (set) {
      set.delete(conn);
      if (set.size === 0) this.byRoom.delete(roomId);
    }
  }

  // ------------------------------------------------ presence 状态机

  /**
   * 一条连接进入房间的 presence 处理：
   * - 该用户在房间无记录：首个设备上线 → 广播 online；
   * - 已 online：重复入房（如重发 join），幂等忽略；
   * - 处于宽限倒计时：断线后快速重连 → 撤销倒计时，保持 online 且不广播
   *   （对外状态从未翻转，避免抖动跳变）。
   */
  _markOnline(conn, roomId) {
    if (this.closed) return;
    let map = this.presence.get(roomId);
    if (!map) {
      map = new Map();
      this.presence.set(roomId, map);
    }
    const p = map.get(conn.userId);
    if (p) {
      if (p.timer) {
        clearTimeout(p.timer);
        p.timer = null;
      }
      p.name = conn.name;
      p.lastActiveAt = Math.max(p.lastActiveAt, conn.lastActiveAt);
      return;
    }
    map.set(conn.userId, { name: conn.name, lastActiveAt: conn.lastActiveAt, timer: null });
    this._emitPresence({
      roomId, userId: conn.userId, name: conn.name,
      online: true, lastActiveAt: conn.lastActiveAt,
    });
  }

  /**
   * 一条连接离开了房间（主动 leave 或连接关闭/心跳超时后的 close）。
   * 只有该用户在该房间「再无任何连接」时才可能改变聚合状态：
   * 仍有其他设备在线 → 维持 online；否则按场景立即离线或进入宽限。
   */
  _userLeftRoom(conn, roomId, immediate) {
    if (this.closed) return;
    const map = this.presence.get(roomId);
    const p = map && map.get(conn.userId);
    if (!p) return;
    const others = this.byRoom.get(roomId);
    const stillConnected = others && [...others].some((c) => c.userId === conn.userId);
    p.lastActiveAt = Math.max(p.lastActiveAt, conn.lastActiveAt);
    if (stillConnected) return; // 多设备聚合：其他连接还在，不判定离线

    if (immediate) {
      this._deletePresence(roomId, conn.userId, p.lastActiveAt);
      return;
    }
    if (p.timer) return; // 已在宽限中，无需重复计时
    const timer = setTimeout(() => {
      const m = this.presence.get(roomId);
      const cur = m && m.get(conn.userId);
      if (!cur || cur.timer !== timer) return; // 宽限内重连已撤销倒计时
      this._deletePresence(roomId, conn.userId, cur.lastActiveAt);
    }, this.config.offlineGraceMs);
    if (typeof timer.unref === 'function') timer.unref();
    p.timer = timer;
  }

  _deletePresence(roomId, userId, lastActiveAt) {
    const map = this.presence.get(roomId);
    const p = map && map.get(userId);
    const name = p ? p.name : null;
    const ts = p ? Math.max(p.lastActiveAt, lastActiveAt) : lastActiveAt;
    if (map) {
      map.delete(userId);
      if (map.size === 0) this.presence.delete(roomId);
    }
    this._emitPresence({ roomId, userId, name, online: false, lastActiveAt: ts });
  }

  _emitPresence(evt) {
    if (this.closed) return;
    for (const fn of this.presenceListeners) {
      try { fn(evt); } catch (err) { console.error('[presence listener error]', err); }
    }
  }

  /** 订阅 presence 变更（online / offline 事件） */
  onPresence(fn) { this.presenceListeners.add(fn); }
  offPresence(fn) { this.presenceListeners.delete(fn); }

  /**
   * 刷新连接活动时间（收到任意帧 / pong），并同步更新其所在房间的 presence 条目，
   * 使成员查询中的「最近活动时间」反映实时活动。宽限中的连接已不在房间内，不受影响。
   */
  touch(conn, t = now()) {
    conn.touch(t);
    for (const roomId of conn.rooms) {
      const p = this.presence.get(roomId);
      const entry = p && p.get(conn.userId);
      if (entry) entry.lastActiveAt = Math.max(entry.lastActiveAt, t);
    }
  }

  /**
   * 房间内用户实时 presence 快照：userId -> { name, lastActiveAt }。
   * online 与宽限窗口（grace）中的用户都返回 —— 宽限内对外仍表现为在线。
   */
  presenceInRoom(roomId) {
    const map = this.presence.get(roomId);
    const out = new Map();
    if (!map) return out;
    for (const [userId, p] of map) {
      out.set(userId, { name: p.name, lastActiveAt: p.lastActiveAt, inGrace: p.timer !== null });
    }
    return out;
  }

  /**
   * 发送单帧到指定连接。track=true 时登记未 ACK 追踪（用于 msg 类帧）。
   * 背压：未确认积压超过上限时断开连接（客户端重连后走 sync 补发）。
   */
  send(conn, frame, { track = false, roomId = null, seq = null } = {}) {
    if (conn.ws.readyState !== 1 /* OPEN */) return false;
    if (track && conn.unackedCount >= this.config.maxUnackedPerConn) {
      conn.ws.close(1013, 'backpressure: too many unacked messages');
      return false;
    }
    const str = typeof frame === 'string' ? frame : JSON.stringify(frame);
    try {
      conn.ws.send(str);
    } catch {
      return false;
    }
    if (track && roomId != null && seq != null) conn.trackUnacked(roomId, seq, str);
    return true;
  }

  /** 广播到房间所有连接（含发送者的其他设备）。frame 只序列化一次。 */
  broadcast(roomId, frame, { track = false, seq = null } = {}) {
    const set = this.byRoom.get(roomId);
    if (!set) return 0;
    const str = JSON.stringify(frame);
    let delivered = 0;
    for (const conn of set) {
      if (this.send(conn, str, { track, roomId, seq })) delivered++;
    }
    return delivered;
  }

  /** 心跳扫描：超时未 pong 的连接直接 terminate（触发 close 走正常清理） */
  heartbeatSweep() {
    const t = now();
    for (const conn of this.all) {
      if (t - conn.lastPong > this.config.heartbeatTimeoutMs) {
        conn.ws.terminate();
        continue;
      }
      try {
        conn.ws.ping();
      } catch { /* 连接已损坏，等待 close 事件清理 */ }
    }
  }

  /** 重发扫描：超时未 ACK 的消息重发；超过最大重发次数判定连接不可用，断开让客户端重连补发 */
  resendSweep() {
    const { ackResendAfterMs, ackMaxResend } = this.config;
    for (const conn of this.all) {
      for (const entry of conn.pendingResends(ackResendAfterMs)) {
        entry.tries++;
        if (entry.tries > ackMaxResend) {
          conn.ws.close(1011, 'ack timeout');
          break;
        }
        if (conn.ws.readyState === 1) {
          try {
            conn.ws.send(entry.frame);
            entry.lastSent = now();
          } catch { /* 下一轮再处理 */ }
        }
      }
    }
  }

  stats() {
    return {
      connections: this.all.size,
      users: this.byUser.size,
      rooms: this.byRoom.size,
    };
  }

  /** 停机：撤销全部离线宽限定时器，停止 presence 事件外发 */
  stop() {
    for (const map of this.presence.values()) {
      for (const p of map.values()) {
        if (p.timer) clearTimeout(p.timer);
      }
    }
    this.presence.clear();
    this.presenceListeners.clear();
    this.closed = true;
  }
}

module.exports = { Hub, Connection };
