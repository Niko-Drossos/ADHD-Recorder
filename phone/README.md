# Phone recorder (Termux)

An alternative to the Automate flows: one shell script that records 5-minute chunks, uploads them, queues them while offline, and gives you home-screen buttons.

## Install

1. From **F-Droid** (not the Play Store), install: **Termux**, **Termux:API**, **Termux:Boot**, **Termux:Widget**.
2. Turn on Tailscale on the phone.
3. In Termux:
   ```bash
   pkg install openssh
   scp -r average-server-admin@100.82.32.109:docker_compose/ADHD-Recorder/phone ~/recall-phone
   bash ~/recall-phone/install.sh
   ```
   It asks for the server URL and the `INGEST_TOKEN` from the server's `.env`, then checks both.
4. `recall test` records 30 seconds. **Turn the screen off while it records** and talk. If the transcript it prints matches, background recording works on your phone. If it comes back empty, Android is silencing the mic in the background; use Automate instead.
5. `recall start`
6. Open the **Termux:Boot** app once so it starts recall on boot.
7. Add the **Termux:Widget** widget to your home screen.
8. Settings → Apps → **Termux** and **Termux:API** → Battery → **Unrestricted**.

## Use

| Widget button | Command | |
|---|---|---|
| Recall 10 min / 30 min | `recall summary 10` | summary as a notification (spoken, if enabled) |
| Pause 1h | `recall pause 60` | mic stops immediately; server drops audio until then |
| Resume | `recall resume` | recording restarts within a minute |
| Start / Stop recorder | `recall start` / `recall stop` | |

The recorder also shows an ongoing notification with Pause / Resume / Stop buttons.

`recall status` shows whether it's running, how many chunks are queued, whether the server is reachable, and recent log lines.

## Files on the phone

- `~/.config/recall/config`: server URL, token, options (`RECALL_CHUNK_SECONDS`, `RECALL_SPEAK`)
- `~/recall-data/queue/`: chunks waiting to upload, retried after every chunk
- `~/recall-data/failed/`: chunks the server rejected
- `~/recall-data/recall.log`

To update after changing the script on the server, re-run the `scp` and `install.sh` (it keeps your answers).
