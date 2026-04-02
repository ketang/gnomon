import test from "node:test";
import assert from "node:assert/strict";

import {
  buildLaunchCommand,
  keyToSequence,
  parseFixtureReport,
  terminalViewportPixels,
} from "./cli.mjs";

test("parseFixtureReport extracts validate-scale paths", () => {
  const fixture = parseFixtureReport(`
Artifacts root: /tmp/root
Source root: /tmp/root/source
SQLite path: /tmp/root/validation.sqlite3
`);

  assert.deepEqual(fixture, {
    artifactsRoot: "/tmp/root",
    sourceRoot: "/tmp/root/source",
    dbPath: "/tmp/root/validation.sqlite3",
  });
});

test("keyToSequence maps terminal navigation keys", () => {
  assert.equal(keyToSequence("ArrowDown"), "\u001b[B");
  assert.equal(keyToSequence("Enter"), "\r");
  assert.equal(keyToSequence("x"), "x");
});

test("terminalViewportPixels returns positive browser dimensions", () => {
  const viewport = terminalViewportPixels({
    viewport: { cols: 120, rows: 40 },
    render: { fontSize: 14, lineHeight: 1 },
  });

  assert.ok(viewport.width > 1000);
  assert.ok(viewport.height > 600);
});

test("buildLaunchCommand injects fixture db and source root", () => {
  const launch = buildLaunchCommand(
    { launch: { program: "cargo", args: ["run", "-p", "gnomon", "--"] } },
    {
      dbPath: "/tmp/validation.sqlite3",
      sourceRoot: "/tmp/source",
    },
    "/repo",
  );

  assert.equal(launch.command, "cargo");
  assert.deepEqual(launch.args, [
    "run",
    "-p",
    "gnomon",
    "--",
    "--db",
    "/tmp/validation.sqlite3",
    "--source-root",
    "/tmp/source",
  ]);
  assert.equal(launch.cwd, "/repo");
});
