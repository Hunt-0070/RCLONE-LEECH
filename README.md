# Rclone → Telegram Bot

A Telegram bot that downloads files from any [rclone](https://rclone.org)-supported
remote (Google Drive, Dropbox, S3, OneDrive, etc.) and re-uploads them straight into
a Telegram chat — as streamable videos or as plain documents — with live progress,
auto-splitting for large files, optional auto-delete from the remote, and an optional
per-file size cap.

## Features

- 📥 Download any file/folder from an rclone remote and upload it to Telegram
- 🎬 **Video** mode (streamable, with auto-thumbnail via ffmpeg) or 📄 **Document** mode
- 📊 Live progress board — per-file download/upload %, speed, ETA, elapsed time
- ✂️ Auto-splits files bigger than ~2 GB into playable parts (ffmpeg `-ss`/`-fs`)
- 🗑 Optional auto-delete from the remote after a successful upload
- 📏 Optional **max file size cap** — skip files over a configurable size instead of
  downloading them
- ⚡ Configurable concurrency (1–5 files at once)
- 🚦 Configurable rclone bandwidth limit
- 🔁 `/retry` re-runs only the files that failed in the last job
- 🔧 `/setrclone` lets you swap in a new `rclone.conf` directly from Telegram, no
  redeploy needed
- 🌐 Built-in `/health` HTTP endpoint for uptime monitors (Railway, Render, UptimeRobot, etc.)
- 👥 Multi-user support — an owner plus an optional allow-list of extra user IDs
- 💾 Stats (files done/failed, bytes uploaded, FloodWait history) persist across restarts

## Requirements

- Python 3.10+
- [rclone](https://rclone.org/downloads/) installed and on `PATH`
- [ffmpeg](https://ffmpeg.org/) (and `ffprobe`) installed and on `PATH` — used for video
  thumbnails and splitting large files
- A Telegram bot token and Telegram API credentials (see [Setup](#setup) below)

### Python dependencies

```
pyrogram
tgcrypto      # speeds up Pyrogram, recommended
psutil        # optional — enables CPU/RAM stats in /status; bot runs fine without it
```

Install with:

```bash
pip install pyrogram tgcrypto psutil
```

## Setup

### 1. Get Telegram API credentials

1. Go to <https://my.telegram.org/apps> and create an app to get your `API_ID` and
   `API_HASH`.
2. Create a bot with [@BotFather](https://t.me/BotFather) to get your `BOT_TOKEN`.
3. Get your own numeric Telegram user ID (e.g. via [@userinfobot](https://t.me/userinfobot))
   — this is your `OWNER_ID`.
4. Decide where uploaded files should land — a channel or chat ID — this is your
   `DUMP_CHAT_ID`. The bot must be an admin/member of that chat.

### 2. Configure rclone

The bot needs an `rclone.conf` with your remotes already configured. You can provide
it any of these ways:

- **`RCLONE_CONF_URL`** — a URL the bot fetches at startup (a raw GitHub Gist works
  well; plain `gist.github.com/...` links are automatically converted to their `/raw`
  form)
- **`RCLONE_CONF`** — the entire contents of `rclone.conf` pasted as a single env var
  (use `\n` for newlines)
- **Mount it directly** at `/root/.config/rclone/rclone.conf` in the container/host
- **`/setrclone`** — once the bot is running, DM it `/setrclone` and attach your
  `rclone.conf` file; it validates and swaps it in live, keeping a `.bak` of the old one

### 3. Set environment variables

Create a `config.env` file (the bot auto-loads it on startup) or set these in your
hosting platform's environment settings:

```env
# ── Required ──
API_ID=123456
API_HASH=your_api_hash_here
BOT_TOKEN=123456789:your_bot_token_here
OWNER_ID=123456789
DUMP_CHAT_ID=-1001234567890

# ── rclone config (pick ONE) ──
RCLONE_CONF_URL=https://gist.github.com/yourname/yourgistid
# or: RCLONE_CONF=[remote]\ntype = drive\n...

# ── Optional ──
AUTHORIZED_USERS=111111111,222222222
DOWNLOAD_DIR=/tmp/rclone_dl
RCLONE_FLAGS=
SPLIT_SIZE=2097152000
PORT=8080
CONCURRENT_JOBS=1
BW_LIMIT=
STATS_FILE=/tmp/rclone_bot_stats.json
```

### 4. Run it

```bash
python3 bot.py
```

The bot starts polling Telegram and also opens a small HTTP server (default port
`8080`) at `/health` for uptime checks.

## Environment variables reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_ID` | ✅ | — | Telegram API ID from my.telegram.org |
| `API_HASH` | ✅ | — | Telegram API hash from my.telegram.org |
| `BOT_TOKEN` | ✅ | — | Bot token from @BotFather |
| `OWNER_ID` | ✅ | — | Your numeric Telegram user ID; always authorized |
| `DUMP_CHAT_ID` | ✅ | — | Chat/channel ID where uploaded files are sent |
| `RCLONE_CONF_URL` | ⚪ one of these three | — | URL to fetch `rclone.conf` from at startup |
| `RCLONE_CONF` | ⚪ | — | Raw `rclone.conf` contents as a string (`\n` for newlines) |
| *(pre-mounted file)* | ⚪ | — | An `rclone.conf` already present at `/root/.config/rclone/rclone.conf` |
| `AUTHORIZED_USERS` | optional | — | Comma/semicolon-separated extra user IDs allowed to use the bot |
| `DOWNLOAD_DIR` | optional | `/tmp/rclone_dl` | Local scratch directory for in-flight downloads |
| `RCLONE_FLAGS` | optional | — | Extra flags appended to every rclone command |
| `SPLIT_SIZE` | optional | `2097152000` (≈2000 MB) | Files larger than this are auto-split into parts before upload |
| `PORT` / `HEALTH_PORT` | optional | `8080` | Port for the built-in health-check server |
| `CONCURRENT_JOBS` | optional | `1` (clamped 1–5) | How many files to process in parallel |
| `BW_LIMIT` | optional | off | rclone download bandwidth limit, e.g. `8M` |
| `STATS_FILE` | optional | `/tmp/rclone_bot_stats.json` | Where stats are persisted across restarts |
| `BOARD_REFRESH_INTERVAL` | optional | `8.0` | Seconds between live progress-board edits |

## Commands

| Command | Description |
|---|---|
| `/start` | Show the command list and feature summary |
| `/dl <remote:path>` | Start a job — lists files at the given rclone path and prompts for mode |
| `/retry` | Re-run only the files that failed in the last job |
| `/setrclone` | Replace the bot's `rclone.conf` by uploading a new file via Telegram |
| `/queue` | Show which files the current job is processing |
| `/status` | Bot uptime, stats, current settings, and server CPU/RAM/disk usage |
| `/logs` | Last ~30 log lines |
| `/setdelete on\|off` | Toggle deleting files from the remote after a successful upload |
| `/setmaxsize on\|off\|<MB>` | Toggle or configure the max download size cap (skips larger files) |
| `/concurrent <1-5>` | Set how many files are processed in parallel |
| `/setbwlimit <rate>\|off` | Limit rclone's download bandwidth, e.g. `/setbwlimit 8M` |
| `/setrefresh <2-30>` | Seconds between live progress-board updates |
| `/cancel` | Stop the running job gracefully (finishes in-flight files first) |
| `/forcestop` | Stop the running job immediately |
| `/restart` | Restart the bot process (refuses while a job is running) |
| `/stop` | Shut the bot down |

After `/dl`, you'll get an inline keyboard to choose:

- **🎬 Video** or **📄 Document** upload mode
- **🗑 Delete** toggle — on/off for this job
- **📏 Size Cap** toggle — on/off for this job

### Max file size cap

`/setmaxsize` controls whether oversized files are skipped *before* they're
downloaded, so no bandwidth is wasted on files you don't want.

```
/setmaxsize           # show current state
/setmaxsize on        # enable using the current/default limit (2000 MB)
/setmaxsize off       # disable — download files of any size
/setmaxsize 500       # set the limit to 500 MB and enable it
```

The same toggle is also available as a button on the `/dl` mode-picker keyboard, so
you can flip it per-job without typing a command. Skipped files are listed separately
in the job's final summary and don't count against `/retry`.

## How file selection & splitting works

- `/dl` recursively lists video and audio files at the given rclone path
  (`VIDEO_EXTS` / `AUDIO_EXTS` — image files are only sent if encountered as part of
  a folder, in **Video** mode falling back to image upload where relevant)
- Files larger than `SPLIT_SIZE` (default ~2000 MB) are split into independently
  playable parts using ffmpeg before upload, since Telegram bots cap individual
  uploads at 2 GB
- Each part keeps the original file extension and is uploaded as its own message

## Deploying

This bot has no external dependencies beyond `rclone`, `ffmpeg`, and the Python
packages above, so it runs well on most container platforms (Railway, Render, Fly.io,
a VPS, etc.). Make sure:

- `rclone` and `ffmpeg` are installed in the runtime image
- `PORT` (or `HEALTH_PORT`) is set if your platform expects the app to bind a port
  for health checks
- A persistent volume is mounted at `DOWNLOAD_DIR` and `STATS_FILE`'s directory if you
  want stats to survive restarts (not required — the bot works fine without one)

## Notes & limitations

- Only one job per user can run at a time; start a new one with `/cancel` first
- `/retry` only re-runs files that **failed**, not files that were **skipped** by the
  size cap
- The size cap is checked via a single batched `rclone lsjson` listing per job (not
  one rclone call per file), so enabling it adds negligible overhead
- Telegram's bot API caps individual file uploads at 2 GB; this is why large files are
  auto-split rather than uploaded whole
