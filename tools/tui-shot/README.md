# gnomon TUI shot

Portable screenshot harness for the `gnomon` terminal UI.

It runs `gnomon` inside a PTY, renders the terminal through `xterm.js` in
headless Chromium via Playwright, drives a scripted scenario, and writes a PNG
plus JSON metadata file after each capture step.

## Install

```bash
cd tools/tui-shot
npm install
npx playwright install chromium
```

## Run

```bash
cd tools/tui-shot
node src/cli.mjs
```

Artifacts land under `tools/tui-shot/artifacts/<scenario>/`.

## Scenario file

The default scenario lives at `scenarios/basic-drilldown.json`.

Each scenario defines:

- viewport geometry in terminal rows and columns
- terminal render settings
- synthetic fixture profile
- launch command
- a sequence of actions such as `key`, `wait_for_idle`, and `screenshot`
