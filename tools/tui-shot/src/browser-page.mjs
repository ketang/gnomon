export function terminalPageMarkup() {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>gnomon tui shot</title>
    <style>
      html, body {
        margin: 0;
        width: 100%;
        height: 100%;
        background: #0b0b0c;
      }

      body {
        display: grid;
        place-items: center;
      }

      #terminal-shell {
        padding: 20px;
        border-radius: 16px;
        background:
          linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.01)),
          #111214;
        box-shadow: 0 24px 80px rgba(0, 0, 0, 0.5);
      }

      #terminal {
        overflow: hidden;
      }
    </style>
  </head>
  <body>
    <div id="terminal-shell">
      <div id="terminal"></div>
    </div>
  </body>
</html>`;
}
