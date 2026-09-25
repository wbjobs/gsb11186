import { LOG_LEVELS } from './log-core.js';
import { BroadcastTransport, RealtimeLogSync } from './log-sync.js';

const ROW_HEIGHT = 28;
const OVERSCAN = 6;
const CHART_SECONDS = 60;

const elements = {
  syncStatus: document.querySelector('#syncStatus'),
  peerCount: document.querySelector('#peerCount'),
  tabLabel: document.querySelector('#tabLabel'),
  levelSelect: document.querySelector('#levelSelect'),
  messageInput: document.querySelector('#messageInput'),
  writeButton: document.querySelector('#writeButton'),
  autoButton: document.querySelector('#autoButton'),
  burstButton: document.querySelector('#burstButton'),
  filterSelect: document.querySelector('#filterSelect'),
  clearButton: document.querySelector('#clearButton'),
  logStats: document.querySelector('#logStats'),
  dropStats: document.querySelector('#dropStats'),
  pendingStats: document.querySelector('#pendingStats'),
  chart: document.querySelector('#logChart'),
  viewport: document.querySelector('#logViewport'),
  spacer: document.querySelector('#logSpacer'),
  rows: document.querySelector('#logRows'),
  emptyState: document.querySelector('#emptyState'),
};

const levelColors = {
  debug: '#94a3b8',
  info: '#38bdf8',
  warn: '#fbbf24',
  error: '#fb7185',
};

let renderScheduled = false;
let stickToBottom = true;
let autoTimer = null;
let autoCount = 0;
let filteredEntries = [];

const metrics = {
  buckets: Array.from({ length: CHART_SECONDS }, () => ({ second: 0, count: 0 })),
  lastTotal: null,
  lastDropped: null,
};

const sync = new RealtimeLogSync({
  transport: new BroadcastTransport('multi-tab-log-demo'),
  onChange: scheduleRender,
  onPeersChange: scheduleRender,
});

elements.tabLabel.textContent = `标签页 ${shortTabId(sync.tabId)}`;

function shortTabId(tabId) {
  return tabId.slice(0, 8);
}

function scheduleRender() {
  if (renderScheduled) return;
  renderScheduled = true;
  requestAnimationFrame(render);
}

function render() {
  renderScheduled = false;
  updateMetrics();
  renderLogs();
  renderStats();
  renderControls();
  drawChart();
}

function renderControls() {
  const disabled = !sync.joined;
  elements.writeButton.disabled = disabled;
  elements.autoButton.disabled = disabled;
  elements.burstButton.disabled = disabled;
  elements.clearButton.disabled = disabled;
  elements.syncStatus.textContent = sync.joined ? '已同步' : '同步中';
}

function renderStats() {
  const stats = sync.store.stats();
  const visibleCount = filteredEntries.length;
  const filterText =
    elements.filterSelect.value === 'all' ? '' : ` · 筛选后 ${visibleCount} 条`;

  elements.logStats.textContent = `${stats.total} / ${stats.maxLogs} 条${filterText}`;
  elements.dropStats.textContent = `已截断 ${stats.droppedCount} 条`;
  elements.pendingStats.textContent = `待发送 ${sync.pendingEntries.length} 条`;
  elements.peerCount.textContent = `${sync.peers.size + 1} 个标签页在线`;
}

function renderLogs() {
  const filter = elements.filterSelect.value;
  const entries = sync.store.getEntries();
  filteredEntries =
    filter === 'all' ? entries : entries.filter((entry) => entry.level === filter);

  elements.emptyState.classList.toggle('visible', filteredEntries.length === 0);
  elements.spacer.style.height = `${filteredEntries.length * ROW_HEIGHT}px`;

  if (stickToBottom) {
    elements.viewport.scrollTop = elements.viewport.scrollHeight;
  }

  const viewportHeight = elements.viewport.clientHeight;
  const scrollTop = elements.viewport.scrollTop;
  const start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  const end = Math.min(
    filteredEntries.length,
    Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + OVERSCAN,
  );

  elements.rows.style.transform = `translateY(${start * ROW_HEIGHT}px)`;
  elements.rows.replaceChildren();

  const fragment = document.createDocumentFragment();
  for (let index = start; index < end; index += 1) {
    fragment.appendChild(createLogRow(filteredEntries[index]));
  }
  elements.rows.appendChild(fragment);
}

function createLogRow(entry) {
  const row = document.createElement('div');
  row.className = 'log-row';

  const time = document.createElement('span');
  time.className = 'log-time';
  time.textContent = formatTime(entry.ts);

  const level = document.createElement('span');
  level.className = `log-level ${entry.level}`;
  level.textContent = entry.level;

  const tab = document.createElement('span');
  tab.className = 'log-tab';
  tab.textContent =
    entry.tabId === sync.tabId ? '本标签页' : `标签 ${shortTabId(entry.tabId)}`;
  tab.title = entry.tabId;

  const message = document.createElement('span');
  message.className = 'log-message';
  message.textContent = entry.message;
  message.title = entry.message;

  row.append(time, level, tab, message);
  return row;
}

function formatTime(timestamp) {
  const date = new Date(timestamp);
  const time = date.toLocaleTimeString('zh-CN', { hour12: false });
  return `${time}.${String(date.getMilliseconds()).padStart(3, '0')}`;
}

function writeSingleLog() {
  const level = elements.levelSelect.value;
  const message =
    elements.messageInput.value.trim() ||
    `手动日志 ${new Date().toLocaleTimeString('zh-CN', { hour12: false })}`;

  sync.append({ level, message });
  elements.messageInput.value = '';
  elements.messageInput.focus();
}

function writeBurstLogs() {
  const inputs = [];
  for (let index = 0; index < 2_000; index += 1) {
    inputs.push({
      level: LOG_LEVELS[index % LOG_LEVELS.length],
      message: `压测日志 #${index + 1}`,
    });
  }
  sync.appendMany(inputs);
}

function toggleAutoWrite() {
  if (autoTimer) {
    clearInterval(autoTimer);
    autoTimer = null;
    elements.autoButton.classList.remove('active');
    elements.autoButton.textContent = '自动写入';
    return;
  }

  elements.autoButton.classList.add('active');
  elements.autoButton.textContent = '停止自动写入';
  autoTimer = setInterval(() => {
    autoCount += 1;
    sync.append({
      level: LOG_LEVELS[autoCount % LOG_LEVELS.length],
      message: `自动日志 #${autoCount}`,
    });
  }, 20);
}

function updateMetrics() {
  const stats = sync.store.stats();

  if (metrics.lastTotal === null) {
    metrics.lastTotal = stats.total;
    metrics.lastDropped = stats.droppedCount;
    return;
  }

  if (stats.total < metrics.lastTotal || stats.droppedCount < metrics.lastDropped) {
    metrics.lastTotal = stats.total;
    metrics.lastDropped = stats.droppedCount;
    return;
  }

  const delta = stats.total - metrics.lastTotal;
  if (delta > 0) addMetricCount(Date.now(), delta);

  metrics.lastTotal = stats.total;
  metrics.lastDropped = stats.droppedCount;
}

function addMetricCount(timestamp, count) {
  const second = Math.floor(timestamp / 1_000);
  const bucket = metrics.buckets[second % CHART_SECONDS];
  if (bucket.second !== second) {
    bucket.second = second;
    bucket.count = 0;
  }
  bucket.count += count;
}

function drawChart() {
  const canvas = elements.chart;
  const context = canvas.getContext('2d');
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  const ratio = window.devicePixelRatio || 1;

  if (canvas.width !== Math.round(width * ratio)) {
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(height * ratio);
  }

  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  context.clearRect(0, 0, width, height);

  const currentSecond = Math.floor(Date.now() / 1_000);
  const values = [];
  for (let offset = CHART_SECONDS - 1; offset >= 0; offset -= 1) {
    const second = currentSecond - offset;
    const bucket = metrics.buckets[second % CHART_SECONDS];
    values.push(bucket.second === second ? bucket.count : 0);
  }

  const maxValue = Math.max(10, ...values);
  const chartTop = 14;
  const chartBottom = height - 24;
  const chartHeight = chartBottom - chartTop;
  const barWidth = width / CHART_SECONDS;

  context.strokeStyle = 'rgba(148, 163, 184, 0.16)';
  context.lineWidth = 1;
  for (let line = 0; line <= 3; line += 1) {
    const y = chartTop + (chartHeight / 3) * line;
    context.beginPath();
    context.moveTo(0, y);
    context.lineTo(width, y);
    context.stroke();
  }

  values.forEach((value, index) => {
    if (value === 0) return;
    const barHeight = Math.max(2, (value / maxValue) * chartHeight);
    const x = index * barWidth + 1;
    const y = chartBottom - barHeight;
    context.fillStyle = levelColors.info;
    context.fillRect(x, y, Math.max(1, barWidth - 2), barHeight);
  });

  context.fillStyle = '#94a3b8';
  context.font = '12px system-ui, sans-serif';
  context.fillText(`峰值 ${maxValue} 条/秒`, 10, 14);
  context.fillText('60 秒前', 10, height - 8);
  const nowLabel = '现在';
  context.fillText(nowLabel, width - context.measureText(nowLabel).width - 10, height - 8);
}

elements.writeButton.addEventListener('click', writeSingleLog);
elements.messageInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') writeSingleLog();
});
elements.autoButton.addEventListener('click', toggleAutoWrite);
elements.burstButton.addEventListener('click', writeBurstLogs);
elements.clearButton.addEventListener('click', () => sync.clear());
elements.filterSelect.addEventListener('change', () => {
  stickToBottom = true;
  scheduleRender();
});

elements.viewport.addEventListener('scroll', () => {
  const { scrollTop, clientHeight, scrollHeight } = elements.viewport;
  stickToBottom = scrollTop + clientHeight >= scrollHeight - 40;
  scheduleRender();
});

window.addEventListener('resize', scheduleRender);
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'hidden') sync.flushNow();
});

let disposed = false;
function disposeSync() {
  if (disposed) return;
  disposed = true;
  if (autoTimer) clearInterval(autoTimer);
  sync.dispose();
}

window.addEventListener('pagehide', disposeSync);
window.addEventListener('beforeunload', disposeSync);

setInterval(scheduleRender, 1_000);
scheduleRender();
