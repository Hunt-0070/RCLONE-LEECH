# Rclone → Telegram Bot

A Telegram bot that downloads media files from any [rclone](https://rclone.org/) remote and uploads them to a Telegram chat, with live progress, auto-splitting of large files, optional auto-delete, and MongoDB-backed config persistence.

## Features

- ⚡ Concurrent downloads/uploads (1–5 files at once)
- 📊 Live download & upload progress with %, speed, and ETA
- 🎬 Video or 📄 Document upload mode per job
- 🖼 Automatic thumbnails for videos (via ffmpeg)
- ✂️ Auto-split files larger than ~1.99 GB
- 📏 Optional max download size cap (skips oversized files)
- 🗑 Optional auto-delete from the remote after a successful upload
- 🚦 Adjustable rclone bandwidth limit
- 💾 **MongoDB config persistence** — all settings are saved and restored on restart
- ♻️ **Resume-on-startup** — after a dyno restart (e.g. Heroku's daily ~24h cycle), an interrupted job is resumed automatically

## Requirements

- Python 3.10+
- [`rclone`](https://rclone.org/downloads/) installed and on `PATH`
- `ffmpeg` / `ffprobe` installed (for thumbnails and splitting). Optional: if they are missing, oversized files are uploaded as-is instead of being split.
- A MongoDB instance (optional, only needed for config persistence and auto-restart)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Configuration is read from environment variables (or a `config.env` file in the working directory, `KEY=VALUE` per line).

### Required

| Variable | Description |
| --- | --- |
| `API_ID` | Telegram API ID (from https://my.telegram.org) |
| `API_HASH` | Telegram API hash |
| `BOT_TOKEN` | Telegram bot token (from @BotFather) |
| `OWNER_ID` | Your Telegram numeric user ID |
| `DUMP_CHAT_ID` | Chat/channel ID where files are uploaded |

### rclone config (one of)

| Variable | Description |
| --- | --- |
| `RCLONE_CONF_URL` | URL to fetch `rclone.conf` from (e.g. a raw gist) |
| `RCLONE_CONF` | Inline `rclone.conf` contents (use `\n` for newlines) |

If neither is set, the bot looks for an existing `/root/.config/rclone/rclone.conf`. You can also upload a new config at runtime with `/setrclone`.

### Optional

| Variable | Default | Description |
| --- | --- | --- |
| `MONGO_URI` | _(empty)_ | MongoDB connection string. Enables config persistence + auto-restart |
| `MONGO_DB_NAME` | `rclone_bot` | MongoDB database name |
| `AUTHORIZED_USERS` | _(owner only)_ | Comma-separated extra user IDs allowed to use the bot |
| `DOWNLOAD_DIR` | `/tmp/rclone_dl` | Temp directory for downloads |
| `RCLONE_FLAGS` | _(empty)_ | Extra flags appended to rclone commands |
| `SPLIT_SIZE` | `2000 MB` | Max part size before splitting (bytes) |
| `PORT` / `HEALTH_PORT` | `8080` | Port for the `/health` HTTP endpoint |
| `CONCURRENT_JOBS` | `1` | Default concurrent files (1–5) |
| `BW_LIMIT` | _(off)_ | rclone bandwidth limit, e.g. `8M` |
| `STATS_FILE` | `/tmp/rclone_bot_stats.json` | Where counters are persisted |
| `BOARD_REFRESH_INTERVAL` | `8.0` | Seconds between progress-board edits |

> When `MONGO_URI` is **not** set, the bot runs exactly as before — persistence and auto-restart are silently disabled.

## Running

```bash
python bot.py
```

A health endpoint is exposed at `http://<host>:<PORT>/health`.

### Docker

```bash
docker build -t rclone-tg-bot .
docker run --env-file config.env -p 8080:8080 rclone-tg-bot
```

## Commands

| Command | Description |
| --- | --- |
| `/dl <remote:path/>` | List a remote path and start a download/upload job |
| `/retry` | Re-run files that failed in the last job |
| `/setrclone` | Upload a new `rclone.conf` (send the file after running this) |
| `/queue` | Show current job progress |
| `/setdelete on\|off` | Auto-delete from remote after upload |
| `/setmaxsize on\|off\|<MB>` | Limit downloads to N MB (default 2000) |
| `/concurrent <1-5>` | Set concurrent jobs |
| `/setbwlimit <8M\|off>` | Limit rclone download bandwidth |
| `/setrefresh <2-30>` | Set progress-board refresh interval (seconds) |
| `/autorestart on\|off` | Auto-resume an interrupted job on next startup (needs MongoDB) |
| `/status` | Bot stats & health |
| `/logs` | Last 30 log lines |
| `/cancel` | Stop current job gracefully (finishes current files) |
| `/forcestop` | Stop the running job immediately |
| `/stop` | Shut down the bot |
| `/restart` | Restart the bot process |

## Reliability notes

- **Graceful split fallback** — if `ffmpeg`/`ffprobe` (or `taskset`, used when `CPU_CORES` is set) are unavailable or fail, the file is uploaded as-is rather than being marked failed.
- **`/retry` honors the original job** — retrying failed files reuses the delete setting the original job ran with, not whatever auto-delete is set to at retry time.
- **Persisted refresh interval** — `BOARD_REFRESH_INTERVAL` set via `/setrefresh` is saved to MongoDB and correctly restored on the next startup.

## Config persistence & auto-restart

When `MONGO_URI` is configured:

- Every settings change (`/setdelete`, `/setmaxsize`, `/concurrent`, `/setbwlimit`, `/setrefresh`, `/autorestart`) is saved to a `config` document in MongoDB and reloaded automatically on the next start.
- The most recent job is tracked in a `last_job` document. When `/autorestart on` is enabled, the bot checks **once on startup** whether the last job was still marked `running` (i.e. interrupted by a restart) and, if so, automatically resumes it. This is ideal for Heroku, where the dyno restarts the process every ~24h. Resume notifications are posted to `DUMP_CHAT_ID`.

### Heroku notes

- Heroku restarts the dyno (and re-runs the bot) automatically every ~24h, and wipes the local filesystem. MongoDB is what makes config and the in-progress job survive that restart.
- Set `MONGO_URI` (and optionally `MONGO_DB_NAME`) as Heroku config vars, then enable `/autorestart on` once. After each daily restart the bot comes back up and resumes any interrupted job on its own.

## License

See [LICENSE](LICENSE) if present.
