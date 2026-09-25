'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const html = fs.readFileSync(`${__dirname}/index.html`, 'utf8');
const start = html.indexOf('  function concurrencyInputAction(');
const end = html.indexOf('  // resetLive', start);
assert.ok(start >= 0 && end > start, 'dashboard quota and worker renderers exist');

function element() {
  const classes = new Set();
  return {
    children: [],
    style: {},
    textContent: '',
    classList: {
      add(name) { classes.add(name); },
      remove(name) { classes.delete(name); },
      contains(name) { return classes.has(name); },
    },
    appendChild(child) { this.children.push(child); },
  };
}
class FixedDate extends Date {
  static now() { return Date.parse('2026-01-01T00:00:00Z'); }
}
const context = {
  Date: FixedDate,
  document: { createElement: () => element() },
};
vm.createContext(context);
vm.runInContext(`${html.slice(start, end)};
  this.concurrencyInputAction = concurrencyInputAction;
  this.showConcurrencyControl = showConcurrencyControl;
  this.showsWorkerColumn = showsWorkerColumn;
  this.workerPresentation = workerPresentation;
  this.applyWorkerPresentation = applyWorkerPresentation;
  this.renderQuota = renderQuota;
  this.formatReset = formatReset;`, context);

for (const [utilization, expected] of [[0.995, '99%'], [1, '100%']]) {
  const cell = element();
  context.renderQuota(cell, utilization);
  const label = cell.children[0].children[1].textContent;
  assert.equal(label, expected, `utilization ${utilization}`);
}
assert.equal(context.formatReset('2026-01-01T01:30:00Z', '5h'), '1h 30m');

assert.equal(context.showConcurrencyControl(1), false, 'single-member pool has no concurrency control');
assert.equal(context.showConcurrencyControl(2), true, 'multi-member pool has concurrency control');
assert.equal(context.showsWorkerColumn(1), false, 'concurrency 1 hides worker column');
assert.equal(context.showsWorkerColumn(2), true, 'concurrency above 1 shows worker column');

for (const raw of ['0', '4', 'abc']) {
  assert.equal(context.concurrencyInputAction(raw, 2, 3).kind, 'invalid', `reject ${raw} without posting`);
}
assert.equal(context.concurrencyInputAction(null, 2, 3).kind, 'ignore', 'Cancel does nothing');
assert.equal(context.concurrencyInputAction('2', 2, 3).kind, 'ignore', 'unchanged value does nothing');
const validConcurrency = context.concurrencyInputAction('3', 2, 3);
assert.equal(validConcurrency.kind, 'post');
assert.equal(validConcurrency.value, 3);

const serving = context.workerPresentation(2, { in_window: true, workers: ['worker-b', 'worker-a'] });
assert.equal(serving.serving, true);
assert.equal(serving.count, 2);
assert.equal(serving.names, 'worker-a, worker-b');
assert.equal(serving.pending, false);
const pending = context.workerPresentation(2, { in_window: false, workers: ['worker-a'] });
assert.equal(pending.pending, true);
assert.equal(pending.title, 'worker-a (moves on next request)');
const pendingCell = element();
context.applyWorkerPresentation(pendingCell, { in_window: false, workers: ['worker-a'] }, 2);
assert.equal(pendingCell.textContent, '1');
assert.equal(pendingCell.title, 'worker-a (moves on next request)');
assert.equal(pendingCell.classList.contains('pending'), true, 'pending worker count is dimmed');
const single = context.workerPresentation(1, { in_window: true, workers: ['worker-a'] });
assert.equal(single.enabled, false);
assert.equal(single.serving, false);
assert.equal(single.count, 0);

const editStart = html.indexOf('  function editConcurrency(');
const editEnd = html.indexOf('  function toggleMember(', editStart);
assert.ok(editStart >= 0 && editEnd > editStart, 'concurrency prompt handler exists');
const requests = [];
const banners = [];
let promptPrefill = null;
context.window = { prompt: (message, value) => { promptPrefill = value; return null; } };
context.showBanner = (message) => banners.push(message);
context.postJSON = (url, body) => {
  requests.push({ url, body });
  return Promise.resolve({ ok: true });
};
context.refreshAfter = () => Promise.resolve();
vm.runInContext(`${html.slice(editStart, editEnd)}; this.editConcurrency = editConcurrency;`, context);
for (const raw of ['0', '4', 'abc']) {
  context.window.prompt = (message, value) => { promptPrefill = value; return raw; };
  context.editConcurrency('auto', 2, 3);
  assert.equal(requests.length, 0, `invalid ${raw} sends no request`);
}
assert.equal(promptPrefill, '2', 'prompt is pre-filled with current concurrency');
context.window.prompt = () => null;
context.editConcurrency('auto', 2, 3);
context.window.prompt = () => '2';
context.editConcurrency('auto', 2, 3);
assert.equal(requests.length, 0, 'Cancel and unchanged input send no request');
context.window.prompt = () => '3';
context.editConcurrency('auto', 2, 3);
assert.equal(requests.length, 1, 'valid changed input sends one request');
assert.equal(requests[0].url, '/_gateway/pool/auto/concurrency');
assert.equal(requests[0].body.concurrency, 3);
assert.equal(banners.length, 3, 'invalid input shows an error');
