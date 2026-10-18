#!/usr/bin/env node
'use strict';

const assert = require('node:assert/strict');
const { platformPackages, resolvePlatformBinary } = require('./agent-quota-gateway.js');

assert.deepEqual(Object.keys(platformPackages).sort(), [
  'darwin-arm64',
  'darwin-x64',
  'linux-arm64',
  'linux-x64',
]);
assert.equal(
  platformPackages['linux-x64'],
  '@shukelabs/agent-quota-gateway-linux-x64',
);
assert.throws(
  () => resolvePlatformBinary('freebsd', 'x64'),
  /platform not supported \(freebsd\/x64\)/,
);
assert.throws(
  () => resolvePlatformBinary('linux', 'ppc64le'),
  /platform not supported \(linux\/ppc64le\)/,
);

console.log('npm launcher assertions passed');
