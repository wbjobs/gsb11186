import assert from 'node:assert/strict';
import { test } from 'node:test';

import { INITIAL_EPOCH, LogStore, LOG_LEVELS } from '../src/log-core.js';
import { BroadcastTransport, RealtimeLogSync } from '../src/log-sync.js';

class FakeBus {
  constructor() {
    this.transports = [];
  }

  register(transport) {
    this.transports.push(transport);
  }

  post(sender, message) {
    for (const transport of [...this.transports]) {
      if (transport === sender || transport.closed) continue;
      transport.onMessage?.(structuredClone(message));
    }
  }
}

class FakeTransport {
  constructor(bus) {
    this.bus = bus;
    this.closed = false;
    this.sent = [];
    this.onMessage = null;
    bus.register(this);
  }

  send(message) {
    this.sent.push(structuredClone(message));
    this.bus.post(this, message);
  }

  close() {
    this.closed = true;
    this.bus.transports = this.bus.transports.filter((transport) => transport !== this);
  }
}

function createNode(bus, tabId) {
  return new RealtimeLogSync({
    tabId,
    transport: new FakeTransport(bus),
    autoStart: false,
  });
}

function flushAll(nodes) {
  nodes.forEach((node) => node.flushNow());
}

function entryId(entry) {
  return `${entry.tabId}:${entry.seq}`;
}

function makeEntry(l, tabId, seq, message = `log-${l}`) {
  return {
    epoch: INITIAL_EPOCH,
    tabId,
    seq,
    l,
    level: LOG_LEVELS[l % LOG_LEVELS.length],
    message,
    ts: 1_700_000_000_000 + l,
  };
}

test('四个标签页同时写入后聚合结果完整且顺序一致', () => {
  const bus = new FakeBus();
  const tabIds = ['tab-a', 'tab-b', 'tab-c', 'tab-d'];
  const nodes = tabIds.map((tabId) => createNode(bus, tabId));

  nodes.forEach((node, nodeIndex) => {
    node.appendMany(
      Array.from({ length: 250 }, (_, index) => ({
        level: LOG_LEVELS[(nodeIndex + index) % LOG_LEVELS.length],
        message: `node-${nodeIndex}-${index}`,
      })),
    );
  });

  flushAll(nodes);

  const expectedIds = new Set(
    tabIds.flatMap((tabId) =>
      Array.from({ length: 250 }, (_, index) => `${tabId}:${index + 1}`),
    ),
  );
  const orderings = nodes.map((node) => node.store.getEntries().map(entryId));

  for (const node of nodes) {
    assert.equal(node.store.stats().total, 1000);
    assert.deepEqual(new Set(node.store.getEntries().map(entryId)), expectedIds);
  }
  assert.ok(orderings.every((order) => JSON.stringify(order) === JSON.stringify(orderings[0])));
});

test('消息乱序和分批到达时不丢失日志', () => {
  const source = new LogStore();
  const entries = Array.from({ length: 100 }, (_, index) =>
    source.createLocalEntry('tab-a', index + 1, {
      level: 'info',
      message: `out-of-order-${index}`,
    }),
  );

  const target = new LogStore();
  target.addEntries(entries.slice(50));
  target.addEntries(entries.slice(0, 50).reverse());

  assert.deepEqual(
    target.getEntries().map((entry) => entry.seq),
    entries.map((entry) => entry.seq),
  );
  assert.equal(target.stats().total, 100);

  const tabIds = ['tab-a', 'tab-b', 'tab-c', 'tab-d'];
  const shuffledEntries = tabIds
    .flatMap((tabId, sourceIndex) => {
      const sourceStore = new LogStore();
      return Array.from({ length: 100 }, (_, index) =>
        sourceStore.createLocalEntry(tabId, index + 1, {
          level: 'debug',
          message: `batch-${sourceIndex}-${index}`,
        }),
      );
    })
    .reverse();

  const shuffledTarget = new LogStore();
  for (let index = 0; index < shuffledEntries.length; index += 37) {
    shuffledTarget.addEntries(shuffledEntries.slice(index, index + 37));
  }

  assert.equal(shuffledTarget.stats().total, 400);
  assert.equal(new Set(shuffledTarget.getEntries().map(entryId)).size, 400);
});

test('超过一万条日志时所有副本保留相同的最新一万条', () => {
  const entries = Array.from({ length: 12_000 }, (_, index) =>
    makeEntry(index + 1, `tab-${index % 4}`, Math.floor(index / 4) + 1),
  );

  const primary = new LogStore({ maxLogs: 10_000 });
  primary.addEntries(entries.slice(0, 6_000));
  primary.addEntries(entries.slice(6_000).reverse());
  primary.addEntries([makeEntry(1, 'tab-old', 1, 'late old entry')]);

  assert.equal(primary.stats().total, 10_000);
  assert.equal(primary.stats().droppedCount, 2_001);
  assert.equal(primary.getEntries()[0].l, 2_001);
  assert.equal(primary.getEntries().at(-1).l, 12_000);

  const reordered = new LogStore({ maxLogs: 10_000 });
  reordered.addEntries([...entries].reverse());
  assert.deepEqual(
    reordered.getEntries().map(entryId),
    primary.getEntries().map(entryId),
  );
});

test('高频写入会合并为广播批次并完整聚合', () => {
  const bus = new FakeBus();
  const writer = createNode(bus, 'tab-writer');
  const reader = createNode(bus, 'tab-reader');

  let localChangeCount = 0;
  writer.onChange = () => {
    localChangeCount += 1;
  };

  writer.appendMany(
    Array.from({ length: 2_000 }, (_, index) => ({
      level: LOG_LEVELS[index % LOG_LEVELS.length],
      message: `burst-${index}`,
    })),
  );
  writer.flushNow();

  const batchMessages = writer.transport.sent.filter((message) => message.type === 'entries');
  assert.equal(localChangeCount, 1);
  assert.equal(batchMessages.length, 4);
  assert.ok(batchMessages.every((message) => message.entries.length <= 500));
  assert.equal(reader.store.stats().total, 2_000);
});

test('clear switches epoch across tabs', () => {
  const bus = new FakeBus();
  const nodes = ['tab-a', 'tab-b', 'tab-c'].map((tabId) => createNode(bus, tabId));

  nodes[0].append({ level: 'info', message: 'before clear' });
  nodes[1].append({ level: 'warn', message: 'before clear' });
  flushAll(nodes);

  nodes[1].clear();

  for (const node of nodes) {
    assert.equal(node.store.stats().total, 0);
    assert.notEqual(node.store.epoch, INITIAL_EPOCH);
  }

  nodes[0].append({ level: 'info', message: 'after clear a' });
  nodes[2].append({ level: 'error', message: 'after clear c' });
  flushAll(nodes);

  for (const node of nodes) {
    assert.equal(node.store.stats().total, 2);
    assert.deepEqual(node.store.getEntries().map(entryId).sort(), ['tab-a:2', 'tab-c:1']);
  }
});

test('concurrent clears converge deterministically', () => {
  const sharedEntry = makeEntry(1, 'tab-x', 1);
  const firstStore = new LogStore();
  const secondStore = new LogStore();
  firstStore.addEntries([sharedEntry]);
  secondStore.addEntries([sharedEntry]);

  const firstClear = firstStore.localClear('tab-a');
  const secondClear = secondStore.localClear('tab-b');

  assert.equal(secondStore.receiveClear(firstClear), false);
  assert.equal(firstStore.receiveClear(secondClear), true);
  assert.equal(firstStore.epoch, secondStore.epoch);
  assert.equal(firstStore.epoch, secondClear.epoch);
});

test('remaining tabs continue after one tab closes', () => {
  const bus = new FakeBus();
  const nodes = ['tab-a', 'tab-b', 'tab-c'].map((tabId) => createNode(bus, tabId));

  nodes.forEach((node) => node.append({ level: 'info', message: 'before close' }));
  flushAll(nodes);

  const closing = nodes[2];
  closing.dispose();

  assert.equal(closing.transport.closed, true);
  assert.equal(nodes[0].peers.size, 1);

  nodes[0].append({ level: 'info', message: 'after close' });
  nodes[0].flushNow();

  assert.equal(nodes[1].store.stats().total, 4);
  assert.equal(nodes[0].store.stats().total, 4);
});

test('a joining tab receives the current epoch and logs by snapshot', () => {
  const bus = new FakeBus();
  const existing = createNode(bus, 'tab-existing');
  const peer = createNode(bus, 'tab-peer');

  existing.appendMany(
    Array.from({ length: 10 }, (_, index) => ({
      level: 'info',
      message: `snapshot-${index}`,
    })),
  );
  existing.flushNow();
  peer.append({ level: 'info', message: 'peer entry' });
  peer.flushNow();

  const joining = createNode(bus, 'tab-joining');
  existing.sendSnapshot(joining.tabId, 'request-1');

  assert.equal(joining.joined, true);
  assert.equal(joining.store.stats().total, 11);
  assert.deepEqual(
    joining.store.getEntries().map(entryId),
    existing.store.getEntries().map(entryId),
  );

  existing.clear();
  existing.append({ level: 'warn', message: 'after clear snapshot' });
  existing.flushNow();

  const clearedJoiner = createNode(bus, 'tab-cleared');
  existing.sendSnapshot(clearedJoiner.tabId, 'request-2');

  assert.equal(clearedJoiner.joined, true);
  assert.notEqual(clearedJoiner.store.epoch, INITIAL_EPOCH);
  assert.equal(clearedJoiner.store.stats().total, 1);
});

test('BroadcastChannel transport delivers batched entries', async () => {
  const channelName = `test-channel-${Date.now()}-${Math.random()}`;
  const writer = new RealtimeLogSync({
    tabId: 'tab-writer',
    transport: new BroadcastTransport(channelName),
    autoStart: false,
  });
  const reader = new RealtimeLogSync({
    tabId: 'tab-reader',
    transport: new BroadcastTransport(channelName),
    autoStart: false,
  });

  writer.appendMany(
    Array.from({ length: 20 }, (_, index) => ({
      level: 'info',
      message: `broadcast-${index}`,
    })),
  );
  writer.flushNow();
  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(reader.store.stats().total, 20);
  writer.dispose();
  reader.dispose();
});
