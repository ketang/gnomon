# Troubleshooting

## Capture Real Terminal Snapshots With Color

If you want a snapshot from a real terminal PTY without using a web terminal or
GUI screenshot, capture the pane output with ANSI escape sequences preserved.

With `tmux`:

```bash
tmux new-session -s gnomon
cargo run -p gnomon
```

In another shell, capture the visible pane with escapes preserved:

```bash
tmux capture-pane -t gnomon:0.0 -e -p > /tmp/gnomon-pane.ansi
```

Useful variants:

```bash
# include scrollback, not just the visible viewport
tmux capture-pane -t gnomon:0.0 -e -S - -p > /tmp/gnomon-full.ansi

# write a plain-text copy without ANSI escapes
tmux capture-pane -t gnomon:0.0 -p > /tmp/gnomon-pane.txt
```

Notes:

- `-e` preserves ANSI color/style escapes.
- `-p` prints the capture to stdout so it can be redirected to a file.
- `-S -` starts at the top of the pane history.
- This produces a terminal snapshot, not a PNG. To turn it into an image, you
  still need a renderer.
