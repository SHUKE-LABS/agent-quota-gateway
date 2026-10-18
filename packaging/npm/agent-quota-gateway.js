#!/usr/bin/env node
'use strict';

const { spawn } = require('node:child_process');

const platformPackages = {
  'linux-x64': '@shukelabs/agent-quota-gateway-linux-x64',
  'linux-arm64': '@shukelabs/agent-quota-gateway-linux-arm64',
  'darwin-arm64': '@shukelabs/agent-quota-gateway-darwin-arm64',
  'darwin-x64': '@shukelabs/agent-quota-gateway-darwin-x64',
};

function resolvePlatformBinary(platform = process.platform, architecture = process.arch) {
  const platformKey = `${platform}-${architecture}`;
  const packageName = platformPackages[platformKey];
  if (!packageName) {
    throw new Error(`platform not supported (${platform}/${architecture})`);
  }

  const binaryName = 'agent-quota-gateway';
  try {
    return {
      packageName,
      binaryPath: require.resolve(`${packageName}/bin/${binaryName}`),
    };
  } catch {
    throw new Error(`platform package ${packageName} is not installed for ${platform}/${architecture}`);
  }
}

function main() {
  let resolved;
  try {
    resolved = resolvePlatformBinary();
  } catch (error) {
    console.error(`agent-quota-gateway: ${error.message}`);
    process.exitCode = 1;
    return;
  }

  const child = spawn(resolved.binaryPath, process.argv.slice(2), { stdio: 'inherit' });
  child.on('error', (error) => {
    console.error(`agent-quota-gateway: failed to start native binary: ${error.message}`);
    process.exitCode = 1;
  });
  child.on('exit', (code) => {
    process.exitCode = code === null ? 1 : code;
  });
}

if (require.main === module) {
  main();
}

module.exports = { platformPackages, resolvePlatformBinary };
