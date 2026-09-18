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
    this.lastActiveAt = now(); // 最近一次活动（建连/pong/任意帧），成员「最近活动时间」依据
    this.rooms = new Set(); // 本连接已加入的房间
    this.unacked = new Map();
    this.unackedCount = 0;
  }

  /** 刷新活动时间：收到任意客户端帧或 pong 时调用 */
  touch(t = now()) {
    if (t > this.lastActiveAt) this.lastActiveAt = t;
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
 * 单个用户的在线状态（多设备聚合）。
 *
 * conns 对应用户全部存活连接：只要还剩一条连接，用户就是在线的；
 * 最后一条连接断开也不立即判离线，而是进入宽限期（graceUntil / roomGrace）——
 * 移动网络抖动、客户端快速重连通常会在数百毫秒内带着同一房间回来，
 * 宽限期内回归则整段抖动对房间内其他成员不可见，避免状态频繁反复跳变。
 *
 * rooms:     该用户至少有一条设备连接在场的房间
 * roomGrace: 房间 -> 宽限截止时间；最后一个在该房间的设备断开后置入，
 *            到期仍未回归才正式发 offline，期间回归则静默撤销
 */
class PresenceState {
  constructor(userId, name) {
    this.userId = userId;
    this.name = name;
    this.conns = new Set();
    this.rooms = new Set();
    this.roomGrace = new Map();
    this.graceUntil = 0; // 全部连接断开后的全局宽限截止时间（0 = 未在宽限）
    this.lastActiveAt = now();
  }

  get isOnline() {
    return this.conns.size > 0;
  }

  /** 该用户所有设备上最新的活动时间 */
  connActiveAt() {
    let t = 0;
    for (const c of this.conns) if (c.lastActiveAt > t) t = c.lastActiveAt;
    return t;
  }
}

/**
 * 连接注册中心：全局/按用户/按房间的连接索引，广播，心跳与重发扫描，
 * 以及成员在线状态（Presence）注册表。
 *
 * Presence 事件通过 emit 回调外抛（server.js 注入），事件：
 *   { kind:'online',  roomId, userId, name, lastActiveAt }
 *   { kind:'offline', roomId, userId, name, lastActiveAt }
 *
 * 不变量：同一 (房间, 用户) 的 online/offline 严格交替；宽限期内的抖动
 * 既不产生 offline 也不重复产生 online。
 */
class Hub {
  constructor(config) {
    this.config = config;
    this.all = new Set(); // 全部连接
    this.byUser = new Map(); // userId -> Set<Connection>
    this.byRoom = new Map(); // roomId -> Set<Connection>
    this.presence = new Map(); // userId -> PresenceState
    this.emit = null; // 由 server.js 注入：(event) => void
  }

  /** 准入控制：全局上限 + 单用户上限。返回 null 表示可接入，否则返回拒绝原因码。 */
  checkAdmission(userId) {
    if (this.all.size >= this.config.maxConnections) return 'SERVER_FULL';
    const mine = this.byUser.get(userId);
    if (mine && mine.size >= this.config.maxConnectionsPerUser) return 'TOO_MANY_DEVICES';
    return null;
  }

  // ------------------------------------------------------------ Presence 内部

  _emit(ev) {
    if (this.emit) this.emit(ev);
  }

  /**
   * 设备进入房间（加入房间事件 / 重连后重新 join）。
   *  - 房间内本就有该用户的其他设备：不通知；
   *  - 处于该房间的断开宽限期（客户端从未收到 offline）：静默撤销，不重复通知；
   *  - 真正的离线 -> 在线回归：发 online。
   */
  _markRoomOnline(p, roomId, t) {
    const wasPresent = p.rooms.has(roomId);
    const inGrace = p.roomGrace.has(roomId);
    p.roomGrace.delete(roomId);
    p.rooms.add(roomId);
    p.graceUntil = 0; // 任意设备回归即终止全局宽限
    if (t > p.lastActiveAt) p.lastActiveAt = t;
    if (!wasPresent && !inGrace) {
      this._emit({ kind: 'online', roomId, userId: p.userId, name: p.name, lastActiveAt: p.lastActiveAt });
    }
  }

  /**
   * 一条设备连接离开房间（主动 leave / 连接关闭）。
   * 仍有其他设备在房间时状态不变；最后一个设备离开则进入房间级宽限期，
   * 不立即发 offline —— 由 presenceSweep 到期处理。
   */
  _markRoomDisconnect(p, roomId, t) {
    for (const c of p.conns) {
      if (c.rooms.has(roomId)) return; // 同账号其他设备仍在房间
    }
    if (!p.rooms.delete(roomId)) return;
    p.roomGrace.set(roomId, t + this.config.presenceGraceMs);
  }

  /**
   * 宽限扫描（定时器驱动）：到期仍未回归的房间正式判离线并通知。
   * 房间级宽限覆盖全部通知；全局宽限只负责翻转聚合在线标志。
   */
  presenceSweep() {
    const t = now();
    for (const p of this.presence.values()) {
      if (p.graceUntil && t >= p.graceUntil) p.graceUntil = 0;
      for (const [roomId, until] of [...p.roomGrace]) {
        if (t < until) continue;
        p.roomGrace.delete(roomId);
        if (p.rooms.has(roomId)) continue; // 宽限到期前已有设备回归（理论上 join 时已清，双保险）
        this._emit({ kind: 'offline', roomId, userId: p.userId, name: p.name, lastActiveAt: p.lastActiveAt });
      }
    }
  }

  /** 收到用户活动（任意帧 / pong）：刷新连接与用户两个层级的活动时间 */
  markActive(conn, t = now()) {
    conn.touch(t);
    const p = this.presence.get(conn.userId);
    if (p && t > p.lastActiveAt) p.lastActiveAt = t;
  }

  // ------------------------------------------------------------ 连接生命周期

  add(conn) {
    this.all.add(conn);
    let set = this.byUser.get(conn.userId);
    if (!set) {
      set = new Set();
      this.byUser.set(conn.userId, set);
    }
    set.add(conn);

    // —— Presence 事件 1：连接建立 ——
    let p = this.presence.get(conn.userId);
    if (!p) {
      p = new PresenceState(conn.userId, conn.name);
      this.presence.set(conn.userId, p);
    }
    p.conns.add(conn);
    p.graceUntil = 0; // 宽限期内新设备接入：撤销待判离线
    if (conn.lastActiveAt > p.lastActiveAt) p.lastActiveAt = conn.lastActiveAt;
    // 连接刚建立时尚未加入任何房间，房间级 online 在 joinRoom 时发出
  }

  remove(conn) {
    this.all.delete(conn);
    const mine = this.byUser.get(conn.userId);
    if (mine) {
      mine.delete(conn);
      if (mine.size === 0) this.byUser.delete(conn.userId);
    }

    // —— Presence 事件 3：连接关闭（事件 4 心跳超时 terminate 后同样触发 close，走同一路径）——
    const p = this.presence.get(conn.userId);
    if (p) {
      const t = now();
      if (conn.lastActiveAt > p.lastActiveAt) p.lastActiveAt = conn.lastActiveAt;
      p.conns.delete(conn);
      // 逐房间结算：同账号还有设备留在房间则继续在线，否则该房间进入宽限
      for (const roomId of conn.rooms) this._markRoomDisconnect(p, roomId, t);
      if (p.conns.size === 0) p.graceUntil = t + this.config.presenceGraceMs;
    }

    for (const roomId of conn.rooms) this._leaveRoomSet(roomId, conn);
    conn.rooms.clear();
    conn.unacked.clear();
    conn.unackedCount = 0;
  }

  joinRoom(conn, roomId) {
    let set = this.byRoom.get(roomId);
    if (!set) {
      set = new Set();
      this.byRoom.set(roomId, set);
    }
    set.add(conn);
    const isNewRoomForConn = !conn.rooms.has(roomId);
    conn.rooms.add(roomId);

    // —— Presence 事件 2：加入房间（重复 join 不重复通知）——
    if (isNewRoomForConn) {
      const p = this.presence.get(conn.userId);
      if (p) this._markRoomOnline(p, roomId, now());
    }
  }

  leaveRoom(conn, roomId) {
    this._leaveRoomSet(roomId, conn);
    conn.rooms.delete(roomId);
    const room = conn.unacked.get(roomId);
    if (room) {
      conn.unackedCount -= room.size;
      conn.unacked.delete(roomId);
    }
    // 主动离房同样走宽限：若客户端立即重新加入，其他成员看不到任何跳变
    const p = this.presence.get(conn.userId);
    if (p) this._markRoomDisconnect(p, roomId, now());
  }

  _leaveRoomSet(roomId, conn) {
    const set = this.byRoom.get(roomId);
    if (set) {
      set.delete(conn);
      if (set.size === 0) this.byRoom.delete(roomId);
    }
  }

  /**
   * 房间成员的在线状态视图（供 members 查询）：Map<userId, {online, lastActiveAt}>。
   *  - 有设备在房间：online=true；
   *  - 房间级宽限未到期：仍报 online=true（查询侧也看不到抖动毛刺）；
   *  - 进程内从未见过 / 宽限已过：不在 Map 中，调用方按 offline 处理。
   */
  roomPresence(roomId) {
    const view = new Map();
    const t = now();
    for (const p of this.presence.values()) {
      const inRoom = p.rooms.has(roomId);
      const graceUntil = p.roomGrace.get(roomId) || 0;
      if (!inRoom && !(graceUntil && t < graceUntil)) continue;
      view.set(p.userId, { online: true, lastActiveAt: p.lastActiveAt });
    }
    return view;
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

  /**
   * 心跳扫描（事件 4：心跳超时）。
   * 超时未 pong 的连接直接 terminate —— 随后触发的 close 事件走 remove()，
   * 与主动断连共用同一条 Presence 清理 + 宽限路径。
   */
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
}

module.exports = { Hub, Connection, PresenceState };
