import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { terminalPageMarkup } from "./browser-page.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TOOL_ROOT = path.resolve(__dirname, "..");
const DEFAULT_SCENARIO = path.join(TOOL_ROOT, "scenarios", "basic-drilldown.json");

export async function main() {
  const cli = parseArgs(process.argv.slice(2));
  if (cli.help) {
    printHelp();
    return;
  }

  const scenario = await readJson(cli.scenarioPath);
  const artifactsDir = path.resolve(cli.artifactsDir, scenario.name);
  await fs.mkdir(artifactsDir, { recursive: true });

  const [{ chromium }, pty] = await Promise.all([
    import("playwright"),
    import("node-pty"),
  ]);
  const fixture = await createFixture(scenario.fixture, cli.repoRoot);
  const launch = buildLaunchCommand(scenario, fixture, cli.repoRoot);
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    deviceScaleFactor: scenario.viewport.deviceScaleFactor ?? 1,
    viewport: terminalViewportPixels(scenario),
  });

  const session = createSession(pty.default, launch, scenario.viewport, cli.repoRoot);
  try {
    await setupTerminalPage(page, scenario);
    bindPtyToPage(page, session);

    for (let index = 0; index < scenario.steps.length; index += 1) {
      const step = scenario.steps[index];
      await runStep({
        page,
        session,
        step,
        index,
        scenario,
        artifactsDir,
        fixture,
        launch,
      });
    }
  } finally {
    session.pty.kill();
    await browser.close();
  }
}

export function parseArgs(argv) {
  let scenarioPath = DEFAULT_SCENARIO;
  let artifactsDir = path.join(TOOL_ROOT, "artifacts");
  let repoRoot = path.resolve(TOOL_ROOT, "..", "..");
  let help = false;

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--help" || arg === "-h") {
      help = true;
    } else if (arg === "--scenario") {
      scenarioPath = path.resolve(argv[index + 1]);
      index += 1;
    } else if (arg === "--artifacts-dir") {
      artifactsDir = path.resolve(argv[index + 1]);
      index += 1;
    } else if (arg === "--repo-root") {
      repoRoot = path.resolve(argv[index + 1]);
      index += 1;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  return { scenarioPath, artifactsDir, repoRoot, help };
}

export function printHelp() {
  console.log(`Usage: node src/cli.mjs [--scenario PATH] [--artifacts-dir PATH] [--repo-root PATH]

Runs a scripted gnomon TUI scenario in a PTY, renders it with xterm.js inside
Chromium, and writes per-step screenshots plus JSON metadata artifacts.`);
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function createFixture(fixture, repoRoot) {
  const baseDir = path.join(TOOL_ROOT, ".tmp");
  await fs.mkdir(baseDir, { recursive: true });
  const artifactsRoot = path.join(baseDir, `fixture-${Date.now()}`);
  await fs.mkdir(artifactsRoot, { recursive: true });

  const { stdout } = await runCommand({
    command: "cargo",
    args: [
      "run",
      "-p",
      "gnomon",
      "--bin",
      "validate-scale",
      "--",
      "--profile",
      fixture.profile ?? "quick",
      "--root",
      artifactsRoot,
      "--keep-artifacts",
    ],
    cwd: repoRoot,
  });

  return parseFixtureReport(stdout);
}

export function parseFixtureReport(stdout) {
  const values = {};
  for (const line of stdout.split("\n")) {
    const delimiter = line.indexOf(":");
    if (delimiter === -1) {
      continue;
    }
    const key = line.slice(0, delimiter).trim();
    const value = line.slice(delimiter + 1).trim();
    values[key] = value;
  }

  if (!values["Artifacts root"] || !values["Source root"] || !values["SQLite path"]) {
    throw new Error(`unable to parse validate-scale output:\n${stdout}`);
  }

  return {
    artifactsRoot: values["Artifacts root"],
    sourceRoot: values["Source root"],
    dbPath: values["SQLite path"],
  };
}

export function buildLaunchCommand(scenario, fixture, repoRoot) {
  const args = [
    ...scenario.launch.args,
    "--db",
    fixture.dbPath,
    "--source-root",
    fixture.sourceRoot,
  ];

  return {
    command: scenario.launch.program,
    args,
    cwd: repoRoot,
    env: {
      ...process.env,
      TERM: "xterm-256color",
      COLORTERM: "truecolor",
    },
  };
}

export function terminalViewportPixels(scenario) {
  const cols = scenario.viewport.cols;
  const rows = scenario.viewport.rows;
  const fontSize = scenario.render.fontSize;
  const width = Math.ceil(cols * fontSize * 0.63 + 80);
  const height = Math.ceil(rows * fontSize * (scenario.render.lineHeight ?? 1) * 1.35 + 80);
  return { width, height };
}

function createSession(pty, launch, viewport, repoRoot) {
  const shell = pty.spawn(launch.command, launch.args, {
    name: "xterm-256color",
    cols: viewport.cols,
    rows: viewport.rows,
    cwd: launch.cwd ?? repoRoot,
    env: launch.env,
  });

  return {
    pty: shell,
    lastOutputAt: Date.now(),
    outputChunks: [],
    exitCode: null,
  };
}

async function setupTerminalPage(page, scenario) {
  const xtermModulePath = resolvePackageFile("@xterm/xterm", "lib/xterm.js");
  const xtermCssPath = resolvePackageFile("@xterm/xterm", "css/xterm.css");

  await page.setContent(terminalPageMarkup());
  await page.addStyleTag({ path: xtermCssPath });
  await page.addScriptTag({ path: xtermModulePath });
  await page.evaluate(({ cols, rows, render }) => {
    const terminal = new window.Terminal({
      cols,
      rows,
      convertEol: true,
      fontFamily: render.fontFamily,
      fontSize: render.fontSize,
      lineHeight: render.lineHeight,
      cursorBlink: false,
      theme: render.theme,
      allowTransparency: false,
    });
    terminal.open(document.getElementById("terminal"));
    window.__GNOMON_TUI_SHOT__ = { terminal };
  }, scenario);
}

function bindPtyToPage(page, session) {
  session.pty.onData(async (data) => {
    session.lastOutputAt = Date.now();
    session.outputChunks.push(data);
    await page.evaluate((chunk) => {
      window.__GNOMON_TUI_SHOT__.terminal.write(chunk);
    }, data);
  });

  session.pty.onExit(({ exitCode }) => {
    session.exitCode = exitCode;
  });
}

async function runStep(context) {
  const { step, page, session } = context;
  if (step.action === "key") {
    session.pty.write(keyToSequence(step.key));
    return;
  }
  if (step.action === "text") {
    session.pty.write(step.text);
    return;
  }
  if (step.action === "sleep") {
    await sleep(step.ms);
    return;
  }
  if (step.action === "wait_for_idle") {
    await waitForIdle(session, step.timeout_ms ?? 3000, step.quiet_ms ?? 200);
    return;
  }
  if (step.action === "screenshot") {
    await captureScreenshot(context);
    return;
  }
  throw new Error(`unsupported action: ${step.action}`);
}

async function captureScreenshot({
  page,
  index,
  step,
  scenario,
  artifactsDir,
  fixture,
  launch,
}) {
  const baseName = `${String(index).padStart(2, "0")}-${step.name}`;
  const pngPath = path.join(artifactsDir, `${baseName}.png`);
  const jsonPath = path.join(artifactsDir, `${baseName}.json`);
  const terminal = page.locator("#terminal-shell");
  await terminal.screenshot({
    path: pngPath,
    animations: "disabled",
    caret: "hide",
  });

  const metadata = {
    scenario: scenario.name,
    step,
    viewport: scenario.viewport,
    render: scenario.render,
    fixture,
    launch: {
      command: launch.command,
      args: launch.args,
      cwd: launch.cwd,
    },
    captured_at: new Date().toISOString(),
  };
  await fs.writeFile(jsonPath, JSON.stringify(metadata, null, 2));
}

async function waitForIdle(session, timeoutMs, quietMs) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (session.exitCode !== null) {
      throw new Error(`PTY exited early with code ${session.exitCode}`);
    }
    if (Date.now() - session.lastOutputAt >= quietMs) {
      return;
    }
    await sleep(25);
  }
  throw new Error(`timed out waiting for PTY idle after ${timeoutMs}ms`);
}

export function keyToSequence(key) {
  switch (key) {
    case "ArrowUp":
      return "\u001b[A";
    case "ArrowDown":
      return "\u001b[B";
    case "ArrowRight":
      return "\u001b[C";
    case "ArrowLeft":
      return "\u001b[D";
    case "Enter":
      return "\r";
    case "Backspace":
      return "\u007f";
    case "Tab":
      return "\t";
    case "Escape":
      return "\u001b";
    default:
      if (key.length === 1) {
        return key;
      }
      throw new Error(`unsupported key: ${key}`);
  }
}

function resolvePackageFile(packageName, relativePath) {
  const packageRoot = path.dirname(
    fileURLToPath(import.meta.resolve(`${packageName}/package.json`)),
  );
  return path.join(packageRoot, relativePath);
}

async function runCommand({ command, args, cwd }) {
  if (process.platform === "win32") {
    throw new Error("unsupported platform");
  }
  const child = spawn(command, args, {
    cwd,
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env,
  });

  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });

  const exitCode = await new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", resolve);
  });

  if (exitCode !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed with code ${exitCode}\n${stderr}`);
  }

  return { stdout, stderr };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack : String(error));
    process.exitCode = 1;
  });
}
