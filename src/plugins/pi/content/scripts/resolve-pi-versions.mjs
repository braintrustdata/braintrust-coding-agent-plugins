#!/usr/bin/env node

import { appendFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";

const PI_PACKAGE = "@earendil-works/pi-coding-agent";
const stableVersionPattern = /^\d+\.\d+\.\d+$/;

// Each host is tested against the latest patch of its newest release lines.
// `sources` lists npm names the host has published under; older names are
// installed under the first (current) name with an npm alias.
const hosts = [
  {
    host: "pi",
    label: "Pi",
    bin: "pi",
    sources: [PI_PACKAGE, "@mariozechner/pi-coding-agent"],
    count: positiveInteger("PI_VERSION_COUNT", "5"),
  },
  {
    // Oh My Pi is a Bun-based Pi fork that loads Pi extensions through its own
    // compatibility shim, so it needs a separate install and binary.
    host: "omp",
    label: "Oh My Pi",
    bin: "omp",
    sources: ["@oh-my-pi/pi-coding-agent"],
    count: positiveInteger("OMP_VERSION_COUNT", "3"),
  },
];

function positiveInteger(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got: ${process.env[name]}`);
  }
  return value;
}

const npmQueryDirectory = mkdtempSync(join(tmpdir(), "pi-version-query-"));

function npmViewVersions(packageName) {
  const output = execFileSync("npm", ["view", packageName, "versions", "--json"], {
    cwd: npmQueryDirectory,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
  }).trim();

  const parsed = JSON.parse(output);
  const versions = Array.isArray(parsed) ? parsed : [parsed];
  return versions.filter((version) => stableVersionPattern.test(version));
}

function compareVersions(a, b) {
  const aParts = a.split(".").map(Number);
  const bParts = b.split(".").map(Number);

  for (let index = 0; index < 3; index += 1) {
    const delta = aParts[index] - bParts[index];
    if (delta !== 0) {
      return delta;
    }
  }

  return 0;
}

function resolveHost({ host, label, bin, sources, count }) {
  const versionsBySource = sources.map((packageName) => ({
    packageName,
    versions: npmViewVersions(packageName),
  }));
  const availableVersions = [...new Set(versionsBySource.flatMap(({ versions }) => versions))].sort(
    compareVersions,
  );

  const releaseLines = [];
  for (const version of availableVersions) {
    const [major, minor] = version.split(".");
    const line = `${major}.${minor}`;
    const previousReleaseLine = releaseLines.at(-1);

    if (previousReleaseLine?.line === line) {
      previousReleaseLine.version = version;
      continue;
    }

    releaseLines.push({ line, version });
  }

  const selectedVersions = releaseLines.map(({ version }) => version).slice(-count);
  if (selectedVersions.length < count) {
    throw new Error(
      `Expected at least ${count} stable ${label} release lines across ${sources.join(
        "/",
      )}, found ${selectedVersions.length}`,
    );
  }

  const [alias] = sources;
  const installSpecFor = (version) => {
    const source = versionsBySource.find(({ versions }) => versions.includes(version))?.packageName;
    if (!source) {
      throw new Error(`No source package found for ${label} ${version}`);
    }
    return source === alias ? `${alias}@${version}` : `${alias}@npm:${source}@${version}`;
  };

  return selectedVersions.map((version) => ({
    host,
    label,
    bin,
    version,
    piCodingAgentSpec: installSpecFor(version),
  }));
}

const targetsByHost = hosts.map((host) => ({ ...host, targets: resolveHost(host) }));
const selectedTargets = targetsByHost.flatMap(({ targets }) => targets);
const piVersions = targetsByHost
  .find(({ host }) => host === "pi")
  .targets.map(({ version }) => version);

const outputs = {
  versions: JSON.stringify(selectedTargets),
  // `latest` and `oldest` describe upstream Pi only.
  latest: piVersions[piVersions.length - 1],
  oldest: piVersions[0],
};

if (process.env.GITHUB_OUTPUT) {
  for (const [key, value] of Object.entries(outputs)) {
    appendFileSync(process.env.GITHUB_OUTPUT, `${key}=${value}\n`);
  }
}

const summaryLines = targetsByHost.map(
  ({ label, count, targets }) =>
    `- ${label}: latest ${count} stable release line${count === 1 ? "" : "s"}: ${targets
      .map(({ version }) => version)
      .join(", ")}`,
);

if (process.env.GITHUB_STEP_SUMMARY) {
  appendFileSync(
    process.env.GITHUB_STEP_SUMMARY,
    ["## Supported Pi compatibility versions", "", ...summaryLines, ""].join("\n"),
  );
}

console.log(`Resolved Pi compatibility versions:\n${summaryLines.join("\n")}`);
