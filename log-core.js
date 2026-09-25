/*
 * log-core.js — 多标签页日志聚合核心逻辑（无依赖，浏览器 / Node 通用）
 *
 * 设计要点：
 * - 每条日志有全局唯一 id = `${tabId}:${seq}`，用于跨标签页去重（乱序/重复投递不丢不重）。
 * - 排序键为 (time, id)：时间戳相同或乱序到达时仍有确定性顺序。
 * - LogStore 容量上限 MAX_LOGS，超出时从头部（最旧）截断并计数。
 * - clearEpoch：清空操作的时间水位线，晚到的旧日志 / 同步快照若早于水位线则丢弃，
 *   保证「清空」在并发写入与乱序消息下语义正确。
 * - Batcher：高频写入时把多条日志合并成一条广播消息，降低 BroadcastChannel 投递次数。
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.LogCore = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const LEVELS = ['debug', 'info', 'warn', 'error'];
  const MAX_LOGS = 10000;

  function makeLog(tabId, seq, level, msg, time) {
    return {
      id: tabId + ':' + seq,
      tab: tabId,
      seq: seq,
      time: time == null ? Date.now() : time,
      level: LEVELS.includes(level) ? level : 'info',
      msg: String(msg),
    };
  }

  // 排序比较：先按时间，再按 id（id 内含 tabId 与递增 seq，保证全局确定序）
  function compareLog(a, b) {
    if (a.time !== b.time) return a.time - b.time;
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
  }

  class LogStore {
    constructor(maxLogs) {
      this.maxLogs = maxLogs || MAX_LOGS;
      this.logs = [];          // 始终按 compareLog 有序
      this.seen = new Set();   // 已接收的日志 id，用于去重
      this.truncated = 0;      // 被截断丢弃的条数
      this.clearEpoch = 0;     // 清空水位线：<= 此时间的日志一律丢弃
    }

    // 添加单条日志；返回 true 表示实际入库
    add(log) {
      if (!log || typeof log.id !== 'string') return false;
      if (this.seen.has(log.id)) return false;          // 重复投递去重
      if (log.time <= this.clearEpoch) return false;    // 清空前/同时的旧日志丢弃
      this.seen.add(log.id);
      // 二分插入保持有序，乱序到达不丢且最终顺序一致
      let lo = 0, hi = this.logs.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (compareLog(this.logs[mid], log) < 0) lo = mid + 1; else hi = mid;
      }
      this.logs.splice(lo, 0, log);
      // 容量截断：丢弃最旧的
      if (this.logs.length > this.maxLogs) {
        const overflow = this.logs.length - this.maxLogs;
        const removed = this.logs.splice(0, overflow);
        for (const r of removed) this.seen.delete(r.id); // 允许截断后同 id 不再占位（实际不会重现）
        this.truncated += overflow;
      }
      return true;
    }

    addMany(logs) {
      let added = 0;
      for (const log of logs) if (this.add(log)) added++;
      return added;
    }

    clear(epoch) {
      const at = epoch == null ? Date.now() : epoch;
      if (at > this.clearEpoch) this.clearEpoch = at;
      this.logs = [];
      this.seen.clear();
      this.truncated = 0;
    }

    filter(level) {
      if (!level || level === 'all') return this.logs.slice();
      return this.logs.filter((l) => l.level === level);
    }

    counts() {
      const c = { all: this.logs.length, debug: 0, info: 0, warn: 0, error: 0 };
      for (const l of this.logs) c[l.level]++;
      return c;
    }

    // 供新标签页同步用：只导出清空水位线之后的日志
    snapshot() {
      return this.logs.slice();
    }
  }

  // 高频写入合并器：把短时间内的多条日志合并为一次广播
  class Batcher {
    constructor(flush, opts) {
      this.flushFn = flush;
      this.interval = (opts && opts.interval) || 100; // 最多每 100ms 发一次
      this.maxBatch = (opts && opts.maxBatch) || 50;  // 队列满 50 条立即发
      this.queue = [];
      this.timer = null;
      this.batchesSent = 0;
      this.logsSent = 0;
    }

    push(log) {
      this.queue.push(log);
      if (this.queue.length >= this.maxBatch) this.flush();
      else if (!this.timer) {
        this.timer = setTimeout(() => this.flush(), this.interval);
        if (typeof this.timer.unref === 'function') this.timer.unref();
      }
    }

    flush() {
      if (this.timer) { clearTimeout(this.timer); this.timer = null; }
      if (this.queue.length === 0) return;
      const batch = this.queue;
      this.queue = [];
      this.batchesSent++;
      this.logsSent += batch.length;
      this.flushFn(batch);
    }
  }

  return { LEVELS, MAX_LOGS, makeLog, compareLog, LogStore, Batcher };
});
