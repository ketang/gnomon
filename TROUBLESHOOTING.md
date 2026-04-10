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

To discover the `-t` target from outside tmux, list panes across all sessions:

```bash
tmux list-panes -a -F '#{session_name}:#{window_index}.#{pane_index} #{pane_current_command} #{pane_title}'
```

Inside tmux, you can run the same capture from the `:` command prompt without
spelling out the current target:

```tmux
capture-pane -e -p > /tmp/gnomon-pane.ansi
```

If you want a tmux key binding for the current visible pane with a dynamic
filename, add this to your `~/.tmux.conf`:

```tmux
bind-key P run-shell 'session=$(tmux display-message -p "#{session_name}" | tr -cs "[:alnum:]._-" "-"); f="/tmp/gnomon-${session}-$(date +%Y%m%d-%H%M%S).ansi"; tmux capture-pane -e -p > "$f"; tmux display-message "wrote $f"'
```

With the default tmux prefix, press `Ctrl-B` and then `Shift-P`. The binding
captures the current visible pane, sanitizes the session name for use in the
filename, writes the ANSI snapshot under `/tmp`, and shows the full path in the
tmux status message.

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
