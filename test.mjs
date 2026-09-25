// test.mjs — 验收测试：模拟多标签页（真实 BroadcastChannel）验证聚合逻辑
// 运行: node test.mjs
import LogCore from './log-core.js';

const { LEVELS, MAX_LOGS, makeLog, LogStore, Batcher } = LogCore;
const CHANNEL = 'multi-tab-log-demo-test-' + process.pid;

let passed = 0, failed = 0;
function assert(cond, name, extra) {
  if (cond) { passed++; console.log('  ✅', name); }
  else { failed++; console.log('  ❌', name, extra ?? ''); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- 模拟一个标签页（与 index.html 中逻辑一致）----
class Tab {
  constructor(id) {
    this.id = id;
    this.seq = 0;
    this.store = new LogStore(MAX_LOGS);
    this.channel = new BroadcastChannel(CHANNEL);
    this.batcher = new Batcher((batch) => {
      this.channel.postMessage({ type: 'logs', from: this.id, logs: batch });
    }, { interval: 50, maxBatch: 50 });
    this.channel.onmessage = (e) => this.onMessage(e.data);
  }
  onMessage(m) {
    if (!m || m.from === this.id) return;
    if (m.type === 'logs') this.store.addMany(m.logs);
    else if (m.type === 'clear') this.store.clear(m.at);
    else if (m.type === 'hello') {
      this.channel.postMessage({ type: 'sync', from: this.id, to: m.from,
        logs: this.store.snapshot(), clearEpoch: this.store.clearEpoch });
    } else if (m.type === 'sync' && m.to === this.id) {
      this.store.clearEpoch = Math.max(this.store.clearEpoch, m.clearEpoch || 0);
      this.store.addMany(m.logs);
    }
  }
  write(level, msg, time) {
    const log = makeLog(this.id, this.seq++, level, msg, time);
    this.store.add(log);
    this.batcher.push(log);
    return log;
  }
  clear() {
    const at = Date.now();
    this.store.clear(at);
    this.channel.postMessage({ type: 'clear', from: this.id, at });
  }
  hello() { this.channel.postMessage({ type: 'hello', from: this.id }); }
  close() { this.batcher.flush(); this.channel.close(); }
}

// ============ 单元：乱序 / 去重 ============
console.log('\n[1] 消息乱序与重复投递');
{
  const s = new LogStore();
  const logs = [];
  for (let i = 0; i < 100; i++) logs.push(makeLog('tabA', i, 'info', 'm' + i, 1000 + i * 10));
  const shuffled = logs.slice().sort(() => Math.random() - 0.5);
  for (const l of shuffled) s.add(l);
  for (const l of shuffled) s.add(l); // 重复投递
  assert(s.logs.length === 100, '乱序+重复后仍 100 条', `got ${s.logs.length}`);
  const sorted = logs.slice().sort(LogCore.compareLog);
  assert(s.logs.every((l, i) => l.id === sorted[i].id), '乱序到达后顺序与全局排序一致');
  // 跨标签页同时间戳
  const s2 = new LogStore();
  const a = [makeLog('tabA', 0, 'info', 'a', 5000), makeLog('tabB', 0, 'info', 'b', 5000),
             makeLog('tabA', 1, 'info', 'c', 4999)];
  s2.add(a[1]); s2.add(a[2]); s2.add(a[0]);
  assert(s2.logs.length === 3 && s2.logs[0].msg === 'c', '同时间戳/时间回退乱序不丢');
}

// ============ 单元：截断 ============
console.log('\n[2] 超过 1 万条截断');
{
  const s = new LogStore();
  for (let i = 0; i < 15000; i++) s.add(makeLog('tabA', i, 'info', 'm' + i, 1000 + i));
  assert(s.logs.length === MAX_LOGS, `长度截断到 ${MAX_LOGS}`, `got ${s.logs.length}`);
  assert(s.truncated === 5000, '截断计数 5000', `got ${s.truncated}`);
  assert(s.logs[0].msg === 'm5000' && s.logs.at(-1).msg === 'm14999', '保留最新、丢弃最旧');
}

// ============ 单元：筛选 / 清空 / 水位线 ============
console.log('\n[3] 筛选与清空');
{
  const s = new LogStore();
  LEVELS.forEach((lv, i) => s.add(makeLog('t', i, lv, lv, 1000 + i)));
  assert(s.filter('warn').length === 1 && s.filter('all').length === 4, '按级别筛选正确');
  const epoch = 2000;
  s.clear(epoch);
  assert(s.logs.length === 0, '清空后列表为空');
  assert(!s.add(makeLog('t', 99, 'info', 'late', 1500)), '清空水位线之前的迟到日志被丢弃');
  assert(s.add(makeLog('t', 100, 'info', 'new', 2001)), '清空后的新日志正常入库');
}

// ============ 单元：高频合并 ============
console.log('\n[4] 高频写入自动合并');
{
  const batches = [];
  const b = new Batcher((batch) => batches.push(batch), { interval: 30, maxBatch: 50 });
  for (let i = 0; i < 1000; i++) b.push(makeLog('t', i, 'info', 'x'));
  b.flush();
  const total = batches.reduce((n, x) => n + x.length, 0);
  assert(total === 1000, '合并不丢日志', `got ${total}`);
  assert(batches.length <= 21, `1000 条合并为 ≤21 批（实际 ${batches.length} 批）`);
  assert(b.logsSent === 1000 && b.batchesSent === batches.length, '合并统计正确');
}

// ============ 集成：4 标签页并发 ============
console.log('\n[5] 4 个标签页同时写入，聚合完整且一致');
{
  const tabs = [new Tab('tab1'), new Tab('tab2'), new Tab('tab3'), new Tab('tab4')];
  const PER_TAB = 200;
  tabs.forEach((t) => { for (let i = 0; i < PER_TAB; i++) t.write('info', `${t.id}-m${i}`); });
  await sleep(600); // 等待批量广播投递
  const expectIds = new Set();
  tabs.forEach((t) => { for (let i = 0; i < PER_TAB; i++) expectIds.add(`${t.id}:${i}`); });
  for (const t of tabs) {
    assert(t.store.logs.length === PER_TAB * 4, `${t.id} 聚合到 ${PER_TAB * 4} 条`, `got ${t.store.logs.length}`);
    assert(t.store.logs.every((l) => expectIds.has(l.id)), `${t.id} 无未知日志`);
  }
  const ref = tabs[0].store.logs.map((l) => l.id).join(',');
  assert(tabs.every((t) => t.store.logs.map((l) => l.id).join(',') === ref),
    '4 个标签页最终日志顺序完全一致');
  assert(tabs[0].batcher.batchesSent < PER_TAB,
    `高频写入被合并（200 条 → ${tabs[0].batcher.batchesSent} 批）`);

  // ---- 标签页关闭 ----
  console.log('\n[6] 标签页关闭后其余正常');
  tabs[3].close();
  await sleep(50);
  for (let i = 0; i < 3; i++) tabs[i].write('warn', `after-close-${i}`);
  await sleep(300);
  for (let i = 0; i < 3; i++) {
    assert(tabs[i].store.logs.length === PER_TAB * 4 + 3,
      `tab${i + 1} 在 tab4 关闭后仍正常聚合`, `got ${tabs[i].store.logs.length}`);
  }

  // ---- 新标签页同步历史 ----
  console.log('\n[7] 新标签页加入自动同步历史');
  const late = new Tab('tab5');
  late.hello();
  await sleep(300);
  assert(late.store.logs.length === PER_TAB * 4 + 3, '新标签页同步到全部历史', `got ${late.store.logs.length}`);

  // ---- 广播清空 ----
  console.log('\n[8] 清空对所有标签页生效');
  tabs[0].clear();
  await sleep(300);
  for (const t of [...tabs.slice(0, 3), late]) {
    assert(t.store.logs.length === 0, `${t.id} 已清空`, `got ${t.store.logs.length}`);
  }
  tabs[1].write('error', 'post-clear');
  await sleep(300);
  for (const t of [...tabs.slice(0, 3), late]) {
    assert(t.store.logs.length === 1 && t.store.logs[0].msg === 'post-clear',
      `${t.id} 清空后新日志正常聚合`);
  }
  tabs.forEach((t, i) => i < 3 && t.close());
  late.close();
}

console.log(`\n结果: ${passed} 通过, ${failed} 失败`);
process.exit(failed ? 1 : 0);
