'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const html = fs.readFileSync(`${__dirname}/index.html`, 'utf8');
const start = html.indexOf('  function renderQuota(');
const end = html.indexOf('  // resetLive', start);
assert.ok(start >= 0 && end > start, 'dashboard quota renderers exist');

function element() {
  return {
    children: [],
    style: {},
    textContent: '',
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
vm.runInContext(`${html.slice(start, end)}; this.renderQuota = renderQuota; this.formatReset = formatReset;`, context);

for (const [utilization, expected] of [[0.995, '99%'], [1, '100%']]) {
  const cell = element();
  context.renderQuota(cell, utilization);
  const label = cell.children[0].children[1].textContent;
  assert.equal(label, expected, `utilization ${utilization}`);
}
assert.equal(context.formatReset('2026-01-01T01:30:00Z', '5h'), '1h 30m');
