export const MAX_LOGS = 10_000;

export const LOG_LEVELS = ['debug', 'info', 'warn', 'error'];

export const INITIAL_EPOCH = 'epoch:initial:v1';

const MAX_MESSAGE_LENGTH = 1_000;

function entryId(entry) {
  return `${entry.tabId}:${entry.seq}`;
}

export function compareEntries(left, right) {
  if (left.l !== right.l) return left.l - right.l;
  if (left.tabId !== right.tabId) return left.tabId < right.tabId ? -1 : 1;
  return left.seq - right.seq;
}

function isValidEntry(entry, expectedEpoch) {
  return Boolean(
    entry &&
      typeof entry === 'object' &&
      entry.epoch === expectedEpoch &&
      typeof entry.tabId === 'string' &&
      entry.tabId.length > 0 &&
      Number.isSafeInteger(entry.seq) &&
      entry.seq > 0 &&
      Number.isSafeInteger(entry.l) &&
      entry.l >= 0 &&
      LOG_LEVELS.includes(entry.level) &&
      typeof entry.message === 'string' &&
      entry.message.length <= MAX_MESSAGE_LENGTH &&
      Number.isFinite(entry.ts),
  );
}

export class LogStore {
  constructor({ maxLogs = MAX_LOGS, now = Date.now } = {}) {
    this.maxLogs = maxLogs;
    this.now = now;
    this.entryMap = new Map();
    this.entryList = [];
    this.epoch = INITIAL_EPOCH;
    this.clock = 0;
    this.clearL = 0;
    this.clearBy = '';
    this.droppedCount = 0;
  }

  createLocalEntry(tabId, seq, { level, message, ts = this.now() }) {
    this.clock += 1;
    return {
      epoch: this.epoch,
      tabId,
      seq,
      l: this.clock,
      level,
      message,
      ts,
    };
  }

  addEntries(entries) {
    if (!Array.isArray(entries)) return [];

    let observedClock = this.clock;
    for (const entry of entries) {
      if (isValidEntry(entry, this.epoch)) {
        observedClock = Math.max(observedClock, entry.l);
      }
    }
    this.clock = observedClock;

    const fresh = [];
    for (const entry of entries) {
      if (!isValidEntry(entry, this.epoch)) continue;

      const id = entryId(entry);
      if (this.entryMap.has(id)) continue;

      if (
        this.entryList.length === this.maxLogs &&
        compareEntries(entry, this.entryList[0]) < 0
      ) {
        this.droppedCount += 1;
        continue;
      }

      this.entryMap.set(id, entry);
      fresh.push(entry);
    }

    if (fresh.length === 0) return [];

    fresh.sort(compareEntries);

    const merged = [];
    let oldIndex = 0;
    let freshIndex = 0;

    while (oldIndex < this.entryList.length && freshIndex < fresh.length) {
      const oldEntry = this.entryList[oldIndex];
      const freshEntry = fresh[freshIndex];
      if (compareEntries(oldEntry, freshEntry) <= 0) {
        merged.push(oldEntry);
        oldIndex += 1;
      } else {
        merged.push(freshEntry);
        freshIndex += 1;
      }
    }

    while (oldIndex < this.entryList.length) {
      merged.push(this.entryList[oldIndex]);
      oldIndex += 1;
    }

    while (freshIndex < fresh.length) {
      merged.push(fresh[freshIndex]);
      freshIndex += 1;
    }

    let evictedIds = null;
    if (merged.length > this.maxLogs) {
      const excess = merged.length - this.maxLogs;
      const evicted = merged.splice(0, excess);
      evictedIds = new Set(evicted.map(entryId));
      for (const entry of evicted) {
        this.entryMap.delete(entryId(entry));
      }
      this.droppedCount += excess;
    }

    this.entryList = merged;
    return fresh.filter((entry) => !evictedIds || !evictedIds.has(entryId(entry)));
  }

  localClear(tabId) {
    this.clock += 1;
    const event = {
      type: 'clear',
      tabId,
      l: this.clock,
    };
    this.adoptClear(event);
    return {
      ...event,
      epoch: this.epoch,
    };
  }

  receiveClear(event) {
    if (!event || typeof event.tabId !== 'string') return false;
    if (!Number.isSafeInteger(event.l) || event.l <= 0) return false;
    return this.adoptClear(event);
  }

  adoptClear({ l, tabId }) {
    if (l < this.clearL) return false;
    if (l === this.clearL && tabId <= this.clearBy) return false;

    this.clearL = l;
    this.clearBy = tabId;
    this.epoch = `clear:${l}:${tabId}`;
    this.clock = Math.max(this.clock, l);
    this.entryMap.clear();
    this.entryList = [];
    this.droppedCount = 0;
    return true;
  }

  snapshot() {
    return {
      version: 1,
      epoch: this.epoch,
      clock: this.clock,
      clearL: this.clearL,
      clearBy: this.clearBy,
      droppedCount: this.droppedCount,
      entries: this.entryList.map((entry) => ({ ...entry })),
    };
  }

  applySnapshot(snapshot, { replace = false } = {}) {
    if (!snapshot || snapshot.version !== 1 || !Array.isArray(snapshot.entries)) {
      return { status: 'invalid' };
    }

    if (!replace && snapshot.epoch !== this.epoch) {
      return { status: 'ignored' };
    }

    if (replace && snapshot.epoch !== this.epoch) {
      this.entryMap.clear();
      this.entryList = [];
      this.epoch = snapshot.epoch;
      this.clock = snapshot.clock;
      this.clearL = snapshot.clearL;
      this.clearBy = snapshot.clearBy;
      this.droppedCount = snapshot.droppedCount;
    } else {
      this.clock = Math.max(this.clock, snapshot.clock);
      this.droppedCount = Math.max(this.droppedCount, snapshot.droppedCount);
    }

    const added = this.addEntries(snapshot.entries);
    return { status: 'applied', added };
  }

  getEntries() {
    return this.entryList;
  }

  stats() {
    return {
      total: this.entryList.length,
      maxLogs: this.maxLogs,
      droppedCount: this.droppedCount,
      epoch: this.epoch,
      clock: this.clock,
    };
  }
}
