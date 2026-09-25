import { LogStore } from './log-core.js';

const MESSAGE_BATCH_SIZE = 500;

function randomId() {
  if (globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function snapshotIsNewer(currentSnapshotEpoch, snapshotEpoch, clearState) {
  if (snapshotEpoch === currentSnapshotEpoch) return true;
  if (snapshotEpoch.startsWith('clear:')) {
    const [, rawL, tabId = ''] = snapshotEpoch.split(':');
    const l = Number(rawL);
    if (!Number.isSafeInteger(l)) return false;
    if (!clearState.clearL) return true;
    if (l !== clearState.clearL) return l > clearState.clearL;
    return tabId > clearState.clearBy;
  }
  return false;
}

export class BroadcastTransport {
  constructor(channelName = 'shared-log-demo') {
    this.channel = new BroadcastChannel(channelName);
    this.onMessage = null;
    this.channel.addEventListener('message', (event) => {
      this.onMessage?.(event.data);
    });
  }

  send(message) {
    this.channel.postMessage(message);
  }

  close() {
    this.channel.close();
  }
}

export class RealtimeLogSync {
  constructor({
    tabId = randomId(),
    transport,
    store = new LogStore(),
    now = () => Date.now(),
    flushDelay = 40,
    helloDelay = 150,
    pullTimeout = 500,
    heartbeatDelay = 1_000,
    peerTimeout = 3_000,
    autoStart = true,
    onChange = null,
    onPeersChange = null,
  } = {}) {
    if (!transport) throw new Error('transport is required');

    this.tabId = tabId;
    this.transport = transport;
    this.store = store;
    this.now = now;
    this.flushDelay = flushDelay;
    this.helloDelay = helloDelay;
    this.pullTimeout = pullTimeout;
    this.heartbeatDelay = heartbeatDelay;
    this.peerTimeout = peerTimeout;
    this.onChange = onChange;
    this.onPeersChange = onPeersChange;

    this.bootAt = now();
    this.seq = 0;
    this.pendingEntries = [];
    this.flushTimer = null;
    this.heartbeatTimer = null;
    this.joinTimer = null;
    this.pullTimer = null;
    this.pullRequest = null;
    this.peers = new Map();
    this.snapshotParts = new Map();
    this.joined = false;
    this.disposed = false;

    this.transport.onMessage = (message) => this.receive(message);

    if (autoStart) this.start();
  }

  start() {
    this.send({ type: 'hello' });
    this.joinTimer = setTimeout(() => this.finishHello(), this.helloDelay);
    this.heartbeatTimer = setInterval(() => this.heartbeat(), this.heartbeatDelay);
  }

  append(input) {
    return this.appendMany([input])[0] ?? null;
  }

  appendMany(inputs) {
    const entries = inputs.map((input) =>
      this.store.createLocalEntry(this.tabId, ++this.seq, input),
    );
    const added = this.store.addEntries(entries);
    if (added.length > 0) {
      this.pendingEntries.push(...added);
      this.scheduleFlush();
    }
    this.emitChange();
    return entries;
  }

  clear() {
    const event = this.store.localClear(this.tabId);
    this.pendingEntries = [];
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.cancelJoin('joined');
    this.joined = true;
    this.send(event);
    this.emitChange();
  }

  receive(message) {
    if (this.disposed || !message || message.tabId === this.tabId) return;

    if (message.type !== 'snapshot' && message.type !== 'pull-snap') {
      this.rememberPeer(message);
    }

    switch (message.type) {
      case 'hello':
        this.send({ type: 'hello-ack' });
        break;
      case 'hello-ack':
      case 'heartbeat':
        break;
      case 'bye':
        this.peers.delete(message.tabId);
        this.emitPeers();
        break;
      case 'entries':
        this.receiveEntries(message.entries);
        break;
      case 'clear':
        this.receiveClear(message);
        break;
      case 'pull-snap':
        if (message.to === this.tabId) this.sendSnapshot(message.tabId, message.requestId);
        break;
      case 'snapshot':
        if (message.to === this.tabId) this.receiveSnapshotPart(message);
        break;
      default:
        break;
    }
  }

  receiveEntries(entries) {
    const added = this.store.addEntries(entries);
    if (added.length > 0) this.emitChange();
  }

  receiveClear(message) {
    const changed = this.store.receiveClear(message);
    if (!changed) return;

    this.pendingEntries = [];
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.cancelJoin('joined');
    this.joined = true;
    this.emitChange();
  }

  finishHello() {
    this.joinTimer = null;
    const candidates = [...this.peers.values()].sort((left, right) => {
      if (left.bootAt !== right.bootAt) return left.bootAt - right.bootAt;
      return left.tabId < right.tabId ? -1 : 1;
    });

    if (candidates.length === 0) {
      this.joined = true;
      this.emitChange();
      return;
    }

    this.pullCandidates = candidates;
    this.pullNextSnapshot();
  }

  pullNextSnapshot() {
    const peer = this.pullCandidates?.shift();
    if (!peer) {
      this.completeJoin();
      return;
    }

    const requestId = randomId();
    this.pullRequest = { requestId, target: peer.tabId };
    this.send({ type: 'pull-snap', requestId, to: peer.tabId });
    this.pullTimer = setTimeout(() => {
      if (this.pullRequest?.requestId === requestId) this.pullNextSnapshot();
    }, this.pullTimeout);
  }

  sendSnapshot(targetTabId, requestId) {
    const snapshot = this.store.snapshot();
    const parts = Math.max(1, Math.ceil(snapshot.entries.length / MESSAGE_BATCH_SIZE));

    for (let part = 0; part < parts; part += 1) {
      const entries = snapshot.entries.slice(
        part * MESSAGE_BATCH_SIZE,
        (part + 1) * MESSAGE_BATCH_SIZE,
      );
      this.send({
        type: 'snapshot',
        requestId,
        to: targetTabId,
        part,
        parts,
        snapshot: {
          ...snapshot,
          entries,
        },
      });
    }
  }

  receiveSnapshotPart(message) {
    if (!Number.isInteger(message.part) || !Number.isInteger(message.parts)) return;
    if (message.part < 0 || message.part >= message.parts) return;

    let transfer = this.snapshotParts.get(message.requestId);
    if (!transfer) {
      transfer = { parts: message.parts, chunks: new Map() };
      this.snapshotParts.set(message.requestId, transfer);
    }
    if (transfer.parts !== message.parts || transfer.chunks.has(message.part)) return;

    transfer.chunks.set(message.part, message.snapshot);
    if (transfer.chunks.size !== transfer.parts) return;

    const first = transfer.chunks.get(0);
    const entries = [];
    for (let part = 0; part < transfer.parts; part += 1) {
      entries.push(...transfer.chunks.get(part).entries);
    }
    this.snapshotParts.delete(message.requestId);
    this.acceptSnapshot({ ...first, entries });
  }

  acceptSnapshot(snapshot) {
    let result;
    if (!this.joined) {
      const replace =
        snapshot.epoch !== this.store.epoch &&
        snapshotIsNewer(this.store.epoch, snapshot.epoch, this.store);
      result = this.store.applySnapshot(snapshot, { replace });
      if (result.status !== 'ignored') this.completeJoin();
    } else {
      result = this.store.applySnapshot(snapshot, { replace: false });
    }

    if (result.status === 'applied') this.emitChange();
  }

  completeJoin() {
    if (this.joined) return;
    this.joined = true;
    this.pullCandidates = [];
    if (this.pullTimer) {
      clearTimeout(this.pullTimer);
      this.pullTimer = null;
    }
    this.pullRequest = null;
    this.emitChange();
  }

  cancelJoin() {
    if (this.joinTimer) {
      clearTimeout(this.joinTimer);
      this.joinTimer = null;
    }
    if (this.pullTimer) {
      clearTimeout(this.pullTimer);
      this.pullTimer = null;
    }
    this.pullRequest = null;
    this.pullCandidates = [];
  }

  scheduleFlush() {
    if (this.flushTimer || this.disposed) return;
    this.flushTimer = setTimeout(() => this.flushNow(), this.flushDelay);
  }

  flushNow() {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    while (this.pendingEntries.length > 0) {
      const entries = this.pendingEntries.splice(0, MESSAGE_BATCH_SIZE);
      const currentEpoch = this.store.epoch;
      const validEntries = entries.filter((entry) => entry.epoch === currentEpoch);
      if (validEntries.length > 0) {
        this.send({ type: 'entries', entries: validEntries });
      }
    }
  }

  heartbeat() {
    const deadline = this.now() - this.peerTimeout;
    for (const [tabId, peer] of this.peers) {
      if (peer.lastSeen < deadline) this.peers.delete(tabId);
    }
    this.send({ type: 'heartbeat' });
    this.emitPeers();
  }

  rememberPeer(message) {
    if (!message.tabId) return;
    const previous = this.peers.get(message.tabId);
    this.peers.set(message.tabId, {
      tabId: message.tabId,
      bootAt: Number.isFinite(message.bootAt) ? message.bootAt : this.now(),
      lastSeen: this.now(),
    });
    if (!previous) this.emitPeers();
  }

  send(partialMessage) {
    if (this.disposed) return;
    this.transport.send({
      tabId: this.tabId,
      bootAt: this.bootAt,
      ...partialMessage,
    });
  }

  emitChange() {
    this.onChange?.();
  }

  emitPeers() {
    this.onPeersChange?.();
  }

  dispose() {
    if (this.disposed) return;
    this.flushNow();
    this.send({ type: 'bye' });
    this.disposed = true;
    if (this.flushTimer) clearTimeout(this.flushTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.cancelJoin();
    this.transport.close();
  }
}
