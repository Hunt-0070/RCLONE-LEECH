import os
import sys
import asyncio
import subprocess
import logging
import shutil
import time
import threading
import re
import collections
import json
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    import psutil
except ImportError:
    psutil = None

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified

# ── Logging with in-memory buffer ─────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
log_buffer: collections.deque = collections.deque(maxlen=100)


class _BufHandler(logging.Handler):
    def emit(self, record):
        log_buffer.append(self.format(record))


_bh = _BufHandler()
_bh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(_bh)

# ── Config loader ──────────────────────────────────────────────
def _load_env_file(path: str = "config.env") -> None:
    p = Path(path)
    if not p.exists():
        return
    log.info(f"Loading config from {path}")
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


_load_env_file("config.env")

# ── rclone.conf loader ─────────────────────────────────────────
RCLONE_CONF_PATH = Path("/root/.config/rclone/rclone.conf")


def _write_rclone_conf() -> None:
    _target = RCLONE_CONF_PATH

    _url = os.environ.get("RCLONE_CONF_URL", "").strip()
    if _url:
        try:
            import urllib.request
            if "gist.github.com" in _url and "/raw" not in _url:
                _url = _url.rstrip("/") + "/raw"
            with urllib.request.urlopen(_url, timeout=15) as r:
                _data = r.read().decode("utf-8")
            _target.parent.mkdir(parents=True, exist_ok=True)
            _target.write_text(_data, encoding="utf-8")
            os.environ["RCLONE_CONFIG"] = str(_target)
            log.info(f"rclone.conf fetched from URL ({len(_data)} chars)")
            return
        except Exception as e:
            log.error(f"Failed to fetch rclone.conf from URL: {e}")

    _rc_text = os.environ.get("RCLONE_CONF", "").strip()
    if _rc_text:
        _rc_text = _rc_text.replace("\\n", "\n")
        try:
            _target.parent.mkdir(parents=True, exist_ok=True)
            _target.write_text(_rc_text, encoding="utf-8")
            os.environ["RCLONE_CONFIG"] = str(_target)
            log.info(f"rclone.conf written from RCLONE_CONF ({len(_rc_text)} chars)")
            return
        except Exception as e:
            log.error(f"Failed to write rclone.conf: {e}")

    if _target.exists():
        os.environ["RCLONE_CONFIG"] = str(_target)
        log.info(f"rclone.conf found at {_target}")
    else:
        log.warning("No rclone.conf found! Set RCLONE_CONF_URL or RCLONE_CONF.")


_write_rclone_conf()

# ── Required variables ──────────────────────────────────────────
_MISSING = []


def _req(key: str) -> str:
    v = os.environ.get(key, "").strip()
    if not v:
        _MISSING.append(key)
    return v


_API_ID = _req("API_ID")
API_HASH = _req("API_HASH")
BOT_TOKEN = _req("BOT_TOKEN")
_OWNER = _req("OWNER_ID")
_DUMP = _req("DUMP_CHAT_ID")

if _MISSING:
    for k in _MISSING:
        log.critical(f"Missing required variable: {k}")
    raise SystemExit(1)

API_ID = int(_API_ID)
OWNER_ID = int(_OWNER)
DUMP_CHAT_ID = int(_DUMP)

# Multi-user authorization: comma-separated extra user IDs (owner always allowed)
AUTHORIZED_USERS: set[int] = {OWNER_ID}
for _uid in os.environ.get("AUTHORIZED_USERS", "").replace(";", ",").split(","):
    _uid = _uid.strip()
    if _uid:
        try:
            AUTHORIZED_USERS.add(int(_uid))
        except ValueError:
            log.warning(f"Ignoring invalid AUTHORIZED_USERS entry: {_uid}")

# ── Optional variables ──────────────────────────────────────────
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/tmp/rclone_dl").strip()
RCLONE_FLAGS = os.environ.get("RCLONE_FLAGS", "").strip()
SPLIT_SIZE = int(os.environ.get("SPLIT_SIZE", str(2000 * 1024 * 1024)))
HEALTH_PORT = int(os.environ.get("PORT") or os.environ.get("HEALTH_PORT") or 8080)
CONCURRENT_JOBS = max(1, min(5, int(os.environ.get("CONCURRENT_JOBS", 1))))
BW_LIMIT = os.environ.get("BW_LIMIT", "").strip()  # e.g. "8M" or "off"
STATS_FILE = os.environ.get("STATS_FILE", "/tmp/rclone_bot_stats.json").strip()
cores = os.environ.get("CPU_CORES", "0")
threads = int(os.environ.get("CPU_THREADS", os.cpu_count() or 1))

Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
log.info(
    f"Config OK — API_ID={API_ID} OWNER={OWNER_ID} DUMP={DUMP_CHAT_ID} "
    f"PORT={HEALTH_PORT} CONCURRENT={CONCURRENT_JOBS} USERS={sorted(AUTHORIZED_USERS)}"
)

# ── File type sets ─────────────────────────────────────────────
VIDEO_EXTS = {
    "mp4", "mkv", "avi", "mov", "webm", "flv", "ts", "m4v", "3gp", "3g2",
    "wmv", "mpg", "mpeg", "m2ts", "mts", "vob", "divx", "xvid", "rm", "rmvb",
    "ogv", "f4v", "asf", "amv", "m2v", "mp2", "mpe", "mpv", "qt", "yuv", "roq", "nsv"
}
AUDIO_EXTS = {
    "mp3", "flac", "aac", "ogg", "opus", "wav", "m4a", "wma", "aiff", "ape",
    "dsd", "mka", "mpc", "ac3", "dts", "ra", "ram", "tta", "wv",
    "mid", "midi", "amr", "3ga", "awb", "gsm", "spx", "caf", "au"
}
IMAGE_EXTS = {"jpg", "jpeg", "png", "webp", "gif", "bmp", "tiff", "tif", "heic", "heif"}

# ── Global state ─────────────────────────────────────────────
BOT_START_TIME = time.time()
active_sessions: set[int] = set()
cancel_flags: dict[int, bool] = {}
upload_mode: dict[int, str] = {}
awaiting_rclone_conf: set[int] = set()  # users who ran /setrclone and now need to send the file
delete_after_upload: bool = False
max_size_enabled: bool = False
max_size_bytes: int = 2000 * 1024 * 1024  # cap used when the toggle is ON
last_failed: list[str] = []          # files that failed in the last job
last_job_remote: str | None = None   # remote path of the last job (for /retry)
last_job_mode: str = "video"
state_lock = threading.Lock()        # protects global scalars/lists above

stats = {
    "total_done": 0,
    "total_failed": 0,
    "total_bytes": 0,
    "last_file": None,
    "last_error": None,
    "current_files": [],
    "floodwait_count": 0,
    "floodwait_last_secs": 0,
    "floodwait_last_time": None,
}
stats_lock = threading.Lock()


def _persist_stats() -> None:
    """Best-effort persist of stats to disk so /restart keeps counters."""
    try:
        with stats_lock:
            snapshot = dict(stats)
        Path(STATS_FILE).write_text(json.dumps(snapshot), encoding="utf-8")
    except Exception as e:
        log.debug(f"_persist_stats failed: {e}")


def _load_stats() -> None:
    try:
        p = Path(STATS_FILE)
        if not p.exists():
            return
        loaded = json.loads(p.read_text(encoding="utf-8"))
        with stats_lock:
            for k in ("total_done", "total_failed", "total_bytes",
                      "floodwait_count"):
                if isinstance(loaded.get(k), int):
                    stats[k] = loaded[k]
        log.info(f"Restored stats from {STATS_FILE}")
    except Exception as e:
        log.debug(f"_load_stats failed: {e}")


_load_stats()

app = Client(
    "rclone_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=60,
    max_concurrent_transmissions=1,
)

auth_filter = filters.user(list(AUTHORIZED_USERS))

# ── Health check server ───────────────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/ping", "/"):
            uptime = int(time.time() - BOT_START_TIME)
            h, rem = divmod(uptime, 3600)
            m, s = divmod(rem, 60)
            with stats_lock:
                done = stats["total_done"]
                failed = stats["total_failed"]
                tbytes = stats["total_bytes"]
            body = (
                f"status=ok\nuptime={h}h {m}m {s}s\n"
                f"active_jobs={len(active_sessions)}\n"
                f"concurrent_limit={CONCURRENT_JOBS}\n"
                f"files_done={done}\n"
                f"files_failed={failed}\n"
                f"bytes_uploaded={tbytes}\n"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_):
        pass


def _run_health():
    try:
        server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
        server.allow_reuse_address = True
        server.serve_forever()
    except Exception as e:
        log.error(f"Health server failed on port {HEALTH_PORT}: {e}")

# ── UI Helpers ──────────────────────────────────────────────


def fmt_size(size_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def fmt_uptime() -> str:
    u = int(time.time() - BOT_START_TIME)
    h, r = divmod(u, 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s"


def _server_resources() -> str:
    """Build a CPU/RAM/disk usage block for /status. Degrades gracefully."""
    lines = []
    if psutil is not None:
        try:
            # interval=None is non-blocking (returns usage since last call),
            # so it never stalls the event loop. Primed once at startup.
            cpu = psutil.cpu_percent(interval=None)
            cores = psutil.cpu_count(logical=True) or 1
            vm = psutil.virtual_memory()
            lines.append(f"🖥 **CPU:** `{cpu:.0f}%` over `{cores}` core(s)")
            lines.append(
                f"🧠 **RAM:** `{fmt_size(vm.used)} / {fmt_size(vm.total)}` "
                f"(`{vm.percent:.0f}%`)"
            )
        except Exception as e:
            log.debug(f"_server_resources psutil failed: {e}")
    else:
        lines.append("🖥 **CPU/RAM:** `psutil not installed`")

    try:
        du = shutil.disk_usage(DOWNLOAD_DIR)
        pct = du.used * 100 / du.total if du.total else 0
        lines.append(
            f"💾 **Disk:** `{fmt_size(du.used)} / {fmt_size(du.total)}` "
            f"(`{pct:.0f}%`, free `{fmt_size(du.free)}`)"
        )
    except Exception as e:
        log.debug(f"_server_resources disk failed: {e}")

    return "\n".join(lines)


def fmt_eta(seconds: float) -> str:
    """Format seconds into a human-readable ETA."""
    if seconds <= 0 or seconds > 86400:
        return "—"
    h, r = divmod(int(seconds), 3600)
    m, s = divmod(r, 60)
    if h > 0:
        return f"{h}h {m}m"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def progress_bar(done: int, total: int, width: int = 12) -> str:
    """Dot-style progress bar: ● for filled, ○ for empty."""
    if total <= 0:
        return "○" * width
    pct = max(0.0, min(1.0, done / total))
    filled = round(width * pct)
    filled = max(0, min(width, filled))
    return "●" * filled + "○" * (width - filled)


def make_progress_line(
    label: str,
    done_bytes: int,
    total_bytes: int,
    speed_str: str = "",
    eta_str: str = "",
    width: int = 12,
    elapsed_str: str = "",
    file_idx: str = "",
) -> str:
    """Build a single rich progress line for a file operation (tree style)."""
    if total_bytes > 0:
        pct = min(100, int(done_bytes * 100 / total_bytes))
        bar = progress_bar(done_bytes, total_bytes, width)
        processed = fmt_size(done_bytes)
        total_sz = fmt_size(total_bytes)
    else:
        pct = 0
        bar = "○" * width
        processed = fmt_size(done_bytes) if done_bytes else "…"
        total_sz = None

    lines = [label, f"├ [{bar}] » {pct}%", f"├ Processed: {processed}"]
    if total_sz:
        lines.append(f"├ Total Size: {total_sz}")
    if speed_str:
        lines.append(f"├ Speed: {speed_str}")
    if eta_str and eta_str != "—":
        lines.append(f"├ ETA: {eta_str}")
    if elapsed_str:
        lines.append(f"├ Elapsed: {elapsed_str}")
    if file_idx:
        lines.append(f"├ File: {file_idx}")

    # Close the tree branch: the final bullet becomes the corner.
    lines[-1] = lines[-1].replace("├", "└", 1)
    return "\n".join(lines)

# ── rclone progress parser ───────────────────────────────────────
_RCLONE_STATS_RE = re.compile(
    r"(?:Transferred:\s+)?([\d.]+\s*\S+)\s*/\s*([\d.]+\s*\S+),\s*(\d+)%,\s*([\d.]+\s*\S+/s)(?:,\s*ETA\s*(\S+))?",
    re.IGNORECASE,
)


def _parse_rclone_line(line: str) -> dict | None:
    """Parse a rclone --stats progress line. Returns dict or None."""
    m = _RCLONE_STATS_RE.search(line)
    if not m:
        return None
    return {
        "done_str": m.group(1).strip(),
        "total_str": m.group(2).strip(),
        "pct": int(m.group(3)),
        "speed": m.group(4).strip(),
        "eta": (m.group(5) or "").strip(),
    }


def _parse_size_str(s: str) -> int:
    """Convert '123.4 MiB' → bytes (approximate). Handles locale commas."""
    units = {"b": 1, "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3,
             "tib": 1024 ** 4, "kb": 1000, "mb": 1000 ** 2, "gb": 1000 ** 3}
    s = s.strip().replace(",", "")
    for suffix, mult in sorted(units.items(), key=lambda x: -len(x[0])):
        if s.lower().endswith(suffix):
            try:
                return int(float(s[:-len(suffix)].strip()) * mult)
            except ValueError:
                return 0
    try:
        return int(float(s))
    except ValueError:
        return 0

# ── Core operations ──────────────────────────────────────────


def _rclone_env() -> dict:
    return os.environ.copy()


def _bwlimit_args() -> list[str]:
    with state_lock:
        bw = BW_LIMIT
    if bw and bw.lower() != "off":
        return ["--bwlimit", bw]
    return []


async def safe_edit(msg: Message, text: str) -> None:
    try:
        await msg.edit(text)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value + 5)
        try:
            await msg.edit(text)
        except Exception as retry_err:
            log.warning(f"safe_edit retry failed: {retry_err}")
    except Exception as e:
        log.warning(f"safe_edit: {e}")


def rclone_list(remote_path: str) -> list[str]:
    cmd = ["rclone", "lsf", "--files-only", "-R", remote_path]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()
    r = subprocess.run(cmd, capture_output=True, text=True, env=_rclone_env())
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "rclone lsf failed")
    allowed = VIDEO_EXTS | AUDIO_EXTS
    # lsf -R returns paths relative to remote_path already,
    # so fname is "file.mp4" or "subfolder/file.mp4" only.
    return [
        f.strip().lstrip("/") for f in r.stdout.splitlines()
        if f.strip() and f.strip().rsplit(".", 1)[-1].lower() in allowed
    ]


def rclone_size_map(remote_path: str) -> dict[str, int]:
    """Recursively list `remote_path` via `rclone lsjson -R` and return a
    {relative_path: size_bytes} map, mirroring rclone_list's path semantics
    (paths relative to remote_path, no leading slash). One subprocess call
    for the whole job instead of one per file. Returns {} on any failure —
    callers should treat a missing key as 'unknown size'."""
    cmd = ["rclone", "lsjson", "-R", remote_path]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, env=_rclone_env())
        if r.returncode != 0:
            return {}
        data = json.loads(r.stdout)
        if not isinstance(data, list):
            return {}
        out = {}
        for entry in data:
            if entry.get("IsDir"):
                continue
            rel = (entry.get("Path") or "").lstrip("/")
            size = entry.get("Size", -1)
            if rel and isinstance(size, (int, float)) and size >= 0:
                out[rel] = int(size)
        return out
    except Exception as e:
        log.debug(f"rclone_size_map failed for {remote_path}: {e}")
        return {}


def rclone_size(remote_path: str) -> int:
    """Fallback: look up the size of a single remote file by listing its
    parent directory and matching the filename. Used when a precomputed
    size map (rclone_size_map) doesn't have an entry — e.g. /retry, where
    files come from a previous job and weren't re-listed.
    Returns -1 if the size can't be determined (caller should treat that as
    'unknown' and not block the download on it)."""
    if ":" in remote_path:
        remote_part, _, path_part = remote_path.partition(":")
        remote_part += ":"
    else:
        remote_part, path_part = "", remote_path
    parent = path_part.rsplit("/", 1)[0] if "/" in path_part else ""
    target_name = path_part.rsplit("/", 1)[-1]
    parent_remote = remote_part + parent if parent else remote_part

    cmd = ["rclone", "lsjson", parent_remote]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, env=_rclone_env())
        if r.returncode != 0:
            return -1
        data = json.loads(r.stdout)
        if not isinstance(data, list):
            return -1
        for entry in data:
            if entry.get("Name") == target_name and not entry.get("IsDir"):
                size = entry.get("Size", -1)
                return int(size) if isinstance(size, (int, float)) and size >= 0 else -1
    except Exception as e:
        log.debug(f"rclone_size failed for {remote_path}: {e}")
    return -1


def rclone_download(
    remote_path: str,
    dest_dir: str,
    progress_callback=None,   # callable(done_bytes, total_bytes, pct, speed, eta) or None
) -> str:
    """
    Download a file via rclone.
    Parses rclone's --stats output and calls progress_callback with live data.
    """
    # Use `rclone copyto` (file-to-file) instead of `rclone copy`
    # (dir-to-dir) so that a full remote file path works directly
    # without any splitting logic.
    _filename = remote_path.split("/")[-1]
    local_dest = os.path.join(dest_dir, _filename)

    cmd = [
        "rclone", "copyto",
        "--stats=2s",
        "--stats-one-line",
        "--stats-log-level", "NOTICE",
        "--buffer-size", "128M",
        "--multi-thread-streams", "4",
        "--transfers", "1",
    ] + _bwlimit_args() + [
        remote_path, local_dest,
    ]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()
    log.info(f"rclone cmd: {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        universal_newlines=True,
        env=_rclone_env(),
    )

    last_line = ""
    last_progress_call = 0.0
    _ansi_re = re.compile(r"\x1b\[[0-9;]*[A-Za-z]|\r")

    for raw in proc.stderr:
        line = _ansi_re.sub("", raw).strip()
        if not line:
            continue
        last_line = line
        parsed = _parse_rclone_line(line)
        if not parsed:
            log.info(f"rclone: {line}")
        if parsed and progress_callback:
            now = time.monotonic()
            if last_progress_call == 0.0 or now - last_progress_call >= 5.0:
                last_progress_call = now
                done_bytes = _parse_size_str(parsed["done_str"])
                total_bytes = _parse_size_str(parsed["total_str"])
                progress_callback(
                    done_bytes,
                    total_bytes,
                    parsed["pct"],
                    parsed["speed"],
                    parsed["eta"],
                )

    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(last_line or "rclone copyto failed")

    if not os.path.exists(local_dest):
        raise FileNotFoundError(f"Downloaded file not found: {local_dest}")
    return local_dest


def rclone_delete(remote_path: str) -> None:
    cmd = ["rclone", "deletefile", remote_path]
    r = subprocess.run(cmd, capture_output=True, text=True, env=_rclone_env())
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "rclone deletefile failed")
    log.info(f"🗑 Deleted from remote: {remote_path}")


def split_file(local_path: str, split_dir: str, user_id: int = 0) -> list[str]:
    """
    Split a media file into parts using the ffmpeg -ss/-fs seek+size method.
    Each part is a valid, independently-playable file with the original extension.
    Falls back to the original file if it is already within SPLIT_SIZE.
    """
    size = os.path.getsize(local_path)
    if size <= SPLIT_SIZE:
        return [local_path]

    split_dir_path = Path(split_dir)
    split_dir_path.mkdir(parents=True, exist_ok=True)
    filename = os.path.basename(local_path)
    stem, dot_ext = os.path.splitext(filename)
    extension = dot_ext  # e.g. ".mkv"

    # ── Get total duration via ffprobe ──
    probe = subprocess.run(
        [
            "ffprobe", "-hide_banner", "-loglevel", "error",
            "-print_format", "json", "-show_format", local_path,
        ],
        capture_output=True, text=True,
    )
    duration = 0
    try:
        fields = json.loads(probe.stdout).get("format", {})
        duration = round(float(fields.get("duration", 0)))
    except Exception:
        duration = 0

    if duration == 0:
        log.error(f"split_file: cannot get duration for {filename}, skipping split")
        return [local_path]

    split_size = SPLIT_SIZE - 3_000_000   # 3 MB safety margin
    start_time = 0
    i = 1
    parts = (size // SPLIT_SIZE) + 1
    multi_streams = True
    out_paths: list[str] = []

    while i <= parts or start_time < duration - 4:
        if cancel_flags.get(user_id):
            return out_paths or [local_path]
        out_path = str(split_dir_path / f"{stem}.part{i:03}{extension}")

        # Build the command explicitly instead of deleting by magic index.
        prefix = ["taskset", "-c", f"{cores}"] if cores != "0" else []
        head = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-ss", str(start_time),
            "-i", local_path,
            "-fs", str(split_size),
        ]
        maps = ["-map", "0"] if multi_streams else []
        tail = [
            "-map_chapters", "-1",
            "-async", "1",
            "-strict", "-2",
            "-c", "copy",
            "-threads", f"{threads}",
            out_path,
        ]
        cmd = prefix + head + maps + tail

        log.info(f"split_file: part {i} — start={start_time}s → {os.path.basename(out_path)}")
        r = subprocess.run(cmd, capture_output=True, text=True)

        if r.returncode != 0:
            stderr = r.stderr.strip()
            try:
                os.remove(out_path)
            except OSError:
                pass
            if multi_streams:
                log.warning(f"split_file: {stderr} — retrying without -map 0. File: {filename}")
                multi_streams = False
                continue
            else:
                log.warning(
                    f"split_file: {stderr} — unable to split {filename}, "
                    f"uploading as-is if under max size."
                )
                return [local_path]

        out_size = os.path.getsize(out_path)
        if out_size > SPLIT_SIZE:
            new_split_size = split_size - (out_size - SPLIT_SIZE) - 5_000_000
            if new_split_size < 50_000_000:  # 50 MB floor
                log.error(
                    f"split_file: split_size would drop below 50 MB floor for {filename}, "
                    f"uploading original as-is."
                )
                os.remove(out_path)
                return [local_path]
            split_size = new_split_size
            log.warning(
                f"split_file: part size {fmt_size(out_size)} exceeds limit, "
                f"retrying with lower split_size ({fmt_size(split_size)}). File: {filename}"
            )
            os.remove(out_path)
            continue

        probe2 = subprocess.run(
            [
                "ffprobe", "-hide_banner", "-loglevel", "error",
                "-print_format", "json", "-show_format", out_path,
            ],
            capture_output=True, text=True,
        )
        lpd = 0
        try:
            fields2 = json.loads(probe2.stdout).get("format", {})
            lpd = round(float(fields2.get("duration", 0)))
        except Exception:
            lpd = 0

        if lpd == 0:
            log.error(f"split_file: part {i} has zero duration, file may be corrupted: {filename}")
            break
        elif duration == lpd:
            log.warning(
                f"split_file: part duration equals source — stream issue. "
                f"Only one part will be produced for {filename}."
            )
            out_paths.append(out_path)
            break
        elif lpd <= 3:
            os.remove(out_path)
            break

        out_paths.append(out_path)
        start_time += lpd - 3   # 3-second overlap buffer
        i += 1

    if not out_paths:
        log.error(f"split_file: no parts produced for {filename}")
        return [local_path]

    log.info(f"split_file: {filename} → {len(out_paths)} parts")
    return out_paths


async def generate_thumbnail(local_path: str) -> str | None:
    thumb_path = local_path + "_thumb.jpg"
    proc = None
    try:
        probe = await asyncio.create_subprocess_exec(
            "ffprobe", "-hide_banner", "-loglevel", "error",
            "-print_format", "json", "-show_format", local_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(probe.communicate(), timeout=15)
        seek_time = 3  # fallback
        try:
            duration = float(json.loads(stdout).get("format", {}).get("duration", 0))
            if duration > 0:
                seek_time = max(1, int(duration / 2))
        except Exception:
            pass

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-ss", str(seek_time),
            "-i", local_path,
            "-vf", "thumbnail",
            "-q:v", "1",
            "-frames:v", "1",
            "-threads", "1",
            thumb_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=30)
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            return thumb_path
    except asyncio.TimeoutError:
        log.warning(f"Thumbnail timed out, skipping: {os.path.basename(local_path)}")
        if proc and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
    except Exception as e:
        log.warning(f"Thumbnail failed: {e}")
    finally:
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) == 0:
            try:
                os.remove(thumb_path)
            except Exception:
                pass
    return None


async def _send_with_retry(coro_fn, *args, max_retries: int = 5, **kwargs):
    attempts = 0
    while True:
        try:
            return await coro_fn(*args, **kwargs)
        except FloodWait as e:
            attempts += 1
            wait = e.value + 10
            log.warning(f"FloodWait {e.value}s (attempt {attempts}/{max_retries})")
            with stats_lock:
                stats["floodwait_count"] += 1
                stats["floodwait_last_secs"] = e.value
                stats["floodwait_last_time"] = time.time()
            if attempts >= max_retries:
                log.error(f"FloodWait: giving up after {max_retries} retries")
                raise
            await asyncio.sleep(wait)

# ── Build the composite status board ─────────────────────────────────


def _render_board(total_files: int, concurrent: int, progress_map: dict) -> str:
    header = f"📦 **{total_files} file(s)** · ⚡ **{concurrent} concurrent**\n"
    separator = "━" * 24

    # FIX: Strictly match final upload completion states so temporary download markers don't trigger them
    done = sum(1 for v in progress_map.values() if v.startswith(("✅ ", "✅🗑", "✅⚠️")))
    failed = sum(1 for v in progress_map.values() if v.startswith("❌"))
    skipped = sum(1 for v in progress_map.values() if v.startswith("⏭"))

    # FIX: Keep files that are currently processing (downloaded, splitting, etc.) in the active display
    active_entries = {
        k: v for k, v in progress_map.items()
        if not (v.startswith(("✅ ", "✅🗑", "✅⚠️")) or v.startswith("❌") or v.startswith("⏭"))
    }

    if active_entries:
        body = "\n\n".join(active_entries[k] for k in sorted(active_entries))
    else:
        body = "_No active jobs_"

    footer_lines = [f"├ Total:  ✅ {done} | ❌ {failed}" + (f" | ⏭ {skipped}" if skipped else "")]
    if psutil is not None:
        try:
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory().percent
            footer_lines.append(f"├ CPU: {cpu:.0f}%  RAM: {ram:.0f}%")
        except Exception as e:
            log.debug(f"_render_board psutil failed: {e}")

    try:
        du = shutil.disk_usage(DOWNLOAD_DIR)
        footer_lines.append(f"├ FREE: {fmt_size(du.free)} | {fmt_size(du.total)}")
    except Exception as e:
        log.debug(f"_render_board disk failed: {e}")

    footer_lines[-1] = footer_lines[-1].replace("├", "└", 1)

    text = header + "\n" + body + "\n\n" + separator + "\n" + "\n".join(footer_lines)
    if len(text) > 4000:
        truncated = text[:4000]
        cut = truncated.rfind("\n")
        text = (truncated[:cut] if cut > 0 else truncated) + "\n…"
    return text

# ── Board refresh loop ─────────────────────────────────────────
BOARD_REFRESH_INTERVAL = float(os.environ.get("BOARD_REFRESH_INTERVAL", "8.0"))


class _BoardState:
    """Shared state for the per-job board refresh loop."""

    def __init__(self):
        self.dirty = False
        self.running = False
        self._task: asyncio.Task | None = None

    def mark_dirty(self):
        self.dirty = True

    async def start(self, status_msg: Message, total_files: int,
                    concurrent: int, progress_map: dict):
        self.running = True
        self.dirty = True
        self._task = asyncio.create_task(
            self._loop(status_msg, total_files, concurrent, progress_map)
        )

    async def stop(self, status_msg: Message, total_files: int,
                   concurrent: int, progress_map: dict):
        """Stop the loop and do one final edit to show the end state."""
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await asyncio.sleep(2.0)
        if progress_map:
            text = _render_board(total_files, concurrent, progress_map)
            await safe_edit(status_msg, text)

    async def _loop(self, status_msg: Message, total_files: int,
                    concurrent: int, progress_map: dict):
        while self.running:
            await asyncio.sleep(BOARD_REFRESH_INTERVAL)
            if self.dirty and self.running:
                self.dirty = False
                text = _render_board(total_files, concurrent, progress_map)
                await safe_edit(status_msg, text)


async def _push_board(status_msg: Message, total_files: int, concurrent: int,
                      progress_map: dict,
                      board_state: "_BoardState | None" = None) -> None:
    """Mark the board dirty so the refresh loop picks it up."""
    if board_state is not None:
        board_state.mark_dirty()
    else:
        text = _render_board(total_files, concurrent, progress_map)
        await safe_edit(status_msg, text)

# ── Upload a single part with live progress ────────────────────────────


async def upload_part_with_progress(
    part_path: str,
    caption: str,
    mode: str,
    thumb: str | None,
    idx: int,
    total_files: int,
    fname: str,
    part_idx: int,
    total_parts: int,
    file_size: int,
    progress_map: dict,
    status_msg: Message,
    reply_to_message_id: int | None = None,
    board_state: "_BoardState | None" = None,
) -> Message:
    """Send one part to Telegram with a live upload progress bar."""

    loop = asyncio.get_running_loop()

    def _make_label() -> str:
        fname_display = fname.split("/")[-1]
        if total_parts > 1:
            return f"📤 `[{idx}/{total_files}]` `{fname_display}`\n    🧩 Part {part_idx}/{total_parts}"
        return f"📤 `[{idx}/{total_files}]` `{fname_display}`"

    async def progress_handler(current: int, total: int) -> None:
        elapsed = loop.time() - upload_start[0]
        speed_bps = current / elapsed if elapsed > 0 else 0
        remaining = (total - current) / speed_bps if speed_bps > 0 else 0

        speed_str = fmt_size(int(speed_bps)) + "/s" if speed_bps > 0 else ""
        eta_str = fmt_eta(remaining)

        fname_disp = fname.split("/")[-1]
        base_label = f"📤 `[{idx}/{total_files}]` `{fname_disp}`"
        if total_parts > 1:
            pct2 = min(100, int(current * 100 / total)) if total > 0 else 0
            bar2 = progress_bar(current, total)
            lines = [
                base_label,
                f"├ [{bar2}] » {pct2}%",
                f"├ Part {part_idx}/{total_parts}",
                f"├ Processed: {fmt_size(current)}",
                f"├ Total Size: {fmt_size(total)}",
            ]
            if speed_str:
                lines.append(f"├ Speed: {speed_str}")
            if eta_str and eta_str != "—":
                lines.append(f"├ ETA: {eta_str}")
            lines.append(f"├ Elapsed: {fmt_eta(elapsed)}")
            lines.append(f"├ File: {idx}/{total_files}")
            lines[-1] = lines[-1].replace("├", "└", 1)
            progress_map[idx] = "\n".join(lines)
        else:
            progress_map[idx] = make_progress_line(
                base_label, current, total, speed_str, eta_str,
                elapsed_str=fmt_eta(elapsed),
                file_idx=f"{idx}/{total_files}",
            )
        if board_state is not None:
            board_state.mark_dirty()

    progress_map[idx] = make_progress_line(
        _make_label(), 0, file_size, file_idx=f"{idx}/{total_files}"
    )
    if board_state is not None:
        board_state.mark_dirty()

    upload_start = [loop.time()]

    is_split = total_parts > 1
    ext = os.path.basename(part_path).rsplit(".", 1)[-1].lower()

    if mode == "video" and (ext in VIDEO_EXTS or is_split):
        return await _send_with_retry(
            app.send_video, DUMP_CHAT_ID, part_path,
            caption=caption, supports_streaming=True, thumb=thumb,
            reply_to_message_id=reply_to_message_id,
            progress=progress_handler
        )
    elif ext in AUDIO_EXTS and mode != "document":
        return await _send_with_retry(
            app.send_audio, DUMP_CHAT_ID, part_path,
            caption=caption, thumb=thumb,
            reply_to_message_id=reply_to_message_id,
            progress=progress_handler
        )
    elif ext in IMAGE_EXTS and mode != "document":
        return await _send_with_retry(
            app.send_photo, DUMP_CHAT_ID, part_path,
            caption=caption,
            reply_to_message_id=reply_to_message_id,
            progress=progress_handler
        )
    else:
        return await _send_with_retry(
            app.send_document, DUMP_CHAT_ID, part_path,
            caption=caption, thumb=thumb,
            reply_to_message_id=reply_to_message_id,
            progress=progress_handler
        )

# ── Process one file end-to-end ─────────────────────────────────────


async def process_one_file(
    fname: str,
    full_remote: str,
    idx: int,
    total: int,
    mode: str,
    user_id: int,
    semaphore: asyncio.Semaphore,
    progress_map: dict,
    status_msg: Message,
    results: dict,
    job_concurrent: int,
    do_delete: bool = False,
    board_state: "_BoardState | None" = None,
    size_map: dict[str, int] | None = None,
) -> None:

    async with semaphore:
        if cancel_flags.get(user_id):
            return

        file_dir = os.path.join(DOWNLOAD_DIR, f"job_{user_id}_{idx}")
        Path(file_dir).mkdir(parents=True, exist_ok=True)
        ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
        loop = asyncio.get_running_loop()
        thumb_path = None
        split_dir_cleanup: str | None = None

        dl_start = time.monotonic()
        fname_display = fname.split("/")[-1]

        def dl_progress(done_bytes, total_bytes, pct, speed, eta):
            progress_map[idx] = make_progress_line(
                f"⬇️ `[{idx}/{total}]` `{fname_display}`",
                done_bytes, total_bytes,
                speed_str=speed,
                eta_str=eta,
                elapsed_str=fmt_eta(time.monotonic() - dl_start),
                file_idx=f"{idx}/{total}",
            )
            if board_state is not None:
                board_state.mark_dirty()

        progress_map[idx] = f"⬇️ `[{idx}/{total}]` Connecting to `{fname_display}`…"
        await _push_board(status_msg, total, job_concurrent, progress_map,
                          board_state=board_state)

        with state_lock:
            size_cap_on = max_size_enabled
            size_cap = max_size_bytes
        if size_cap_on:
            remote_size = size_map.get(fname, -1) if size_map else -1
            if remote_size < 0:
                # Cache miss (e.g. /retry, where files weren't freshly listed
                # for this job) — fall back to a single per-file lookup.
                remote_size = await loop.run_in_executor(None, rclone_size, full_remote)
            if remote_size >= 0 and remote_size > size_cap:
                log.info(
                    f"[{idx}/{total}] Skipped (size cap): {fname} "
                    f"({fmt_size(remote_size)} > {fmt_size(size_cap)})"
                )
                progress_map[idx] = (
                    f"⏭ `[{idx}/{total}]` Skipped: `{fname_display}`\n"
                    f"    `{fmt_size(remote_size)} exceeds {fmt_size(size_cap)} limit`"
                )
                results["skipped"].append(fname)
                shutil.rmtree(file_dir, ignore_errors=True)
                return

        try:
            local_path = await loop.run_in_executor(
                None, lambda: rclone_download(full_remote, file_dir, dl_progress)
            )
        except Exception as e:
            log.error(f"[{idx}/{total}] Download failed: {fname} — {e}")
            progress_map[idx] = f"❌ `[{idx}/{total}]` DL failed: `{fname_display}`\n    `{str(e)[:80]}`"
            results["failed"].append(fname)
            with stats_lock:
                stats["total_failed"] += 1
                stats["last_error"] = f"DL {fname}: {str(e)[:100]}"
            shutil.rmtree(file_dir, ignore_errors=True)
            return

        file_size = os.path.getsize(local_path)
        dl_elapsed = time.monotonic() - dl_start
        dl_speed = fmt_size(int(file_size / dl_elapsed)) + "/s" if dl_elapsed > 0 else ""

        # FIX: Changed prefix from '✅⬇️' to '📥 Downloaded ✅' so it isn't parsed as a completed upload
        progress_map[idx] = (
            f"📥 Downloaded ✅ `[{idx}/{total}]` `{fname_display}`\n"
            f"└ [{'●' * 12}] » 100%  {fmt_size(file_size)}"
            + (f"  ⚡ avg {dl_speed}" if dl_speed else "")
        )
        await _push_board(status_msg, total, job_concurrent, progress_map,
                          board_state=board_state)

        needs_split = file_size > SPLIT_SIZE

        if ext in VIDEO_EXTS:
            progress_map[idx] = f"🖼 `[{idx}/{total}]` Generating thumbnail for `{fname_display}`…"
            await _push_board(status_msg, total, job_concurrent, progress_map,
                              board_state=board_state)
            thumb_path = await generate_thumbnail(local_path)

        if needs_split:
            progress_map[idx] = (
                f"✂️ `[{idx}/{total}]` Splitting `{fname_display}` "
                f"({fmt_size(file_size)})"
            )
            await _push_board(status_msg, total, job_concurrent, progress_map,
                              board_state=board_state)
            split_dir = local_path + "_parts"
            split_dir_cleanup = split_dir
            parts = await loop.run_in_executor(
                None, split_file, local_path, split_dir, user_id
            )
            if local_path not in parts:
                # Split actually produced separate part files — the original
                # is no longer needed, so free it now instead of holding
                # original + parts on disk simultaneously (was ~2x file size).
                try:
                    os.remove(local_path)
                except OSError as e:
                    log.warning(f"[{idx}/{total}] Could not remove original after split: {e}")
        else:
            parts = [local_path]

        total_parts = len(parts)

        try:
            prev_msg_id: int | None = None
            all_parts_sent = True
            for part_idx, part_path in enumerate(parts, 1):
                part_size = os.path.getsize(part_path)
                fname_only = fname.split("/")[-1]
                # Strip remote prefix e.g. "Dropbox3:filename.mp4" → "filename.mp4"
                if ":" in fname_only:
                    fname_only = fname_only.split(":", 1)[-1]
                part_filename = os.path.basename(part_path)
                if ":" in part_filename:
                    part_filename = part_filename.split(":", 1)[-1]
                if total_parts > 1:
                    caption = (
                        f"`{part_filename}`\n\n"
                        + f"🧩 Part {part_idx}/{total_parts} · {fmt_size(part_size)} | Total: {fmt_size(file_size)}"
                    )
                else:
                    caption = f"`{fname_only}`"

                sent = await upload_part_with_progress(
                    part_path=part_path,
                    caption=caption,
                    mode=mode,
                    thumb=thumb_path,
                    idx=idx,
                    total_files=total,
                    fname=fname,
                    part_idx=part_idx,
                    total_parts=total_parts,
                    file_size=part_size,
                    progress_map=progress_map,
                    status_msg=status_msg,
                    reply_to_message_id=prev_msg_id,
                    board_state=board_state,
                )
                if sent:
                    prev_msg_id = sent.id
                    # Free this part's disk space immediately rather than
                    # holding every part until the whole file finishes
                    # uploading — keeps peak usage to ~1 part at a time.
                    try:
                        os.remove(part_path)
                    except OSError as e:
                        log.warning(
                            f"[{idx}/{total}] Could not remove part after upload: "
                            f"{part_path} — {e}"
                        )
                else:
                    all_parts_sent = False
                    log.error(
                        f"[{idx}/{total}] Part {part_idx}/{total_parts} returned "
                        f"no message for {fname} — upload incomplete."
                    )
                with stats_lock:
                    stats["total_bytes"] += part_size

            if do_delete and not all_parts_sent:
                progress_map[idx] = (
                    f"✅⚠️ `[{idx}/{total}]` Done (del skipped, upload incomplete): "
                    f"`{fname}`"
                )
                results["delete_failed"].append(fname)
                log.warning(
                    f"[{idx}/{total}] Skipping remote delete — upload incomplete: {fname}"
                )
            elif do_delete:
                progress_map[idx] = f"🗑 `[{idx}/{total}]` Deleting from remote…"
                await _push_board(status_msg, total, job_concurrent, progress_map,
                                  board_state=board_state)
                try:
                    await loop.run_in_executor(None, rclone_delete, full_remote)
                    progress_map[idx] = (
                        f"✅🗑 `[{idx}/{total}]` Done + deleted: `{fname}` · {fmt_size(file_size)}"
                    )
                    results["deleted"] += 1
                    log.info(f"[{idx}/{total}] 🗑 Deleted: {fname}")
                except Exception as de:
                    log.error(f"[{idx}/{total}] Delete failed: {fname} — {de}")
                    progress_map[idx] = (
                        f"✅⚠️ `[{idx}/{total}]` Done (del failed): `{fname}`"
                    )
                    results["delete_failed"].append(fname)
            else:
                progress_map[idx] = (
                    f"✅ `[{idx}/{total}]` `{fname_display}` · {fmt_size(file_size)}"
                )

            results["success"] += 1
            with stats_lock:
                stats["total_done"] += 1
                stats["last_file"] = fname
            _persist_stats()
            log.info(f"[{idx}/{total}] ✅ Done: {fname}")
            if board_state is not None:
                board_state.mark_dirty()

        except Exception as e:
            log.error(f"[{idx}/{total}] Upload failed: {fname} — {e}")
            progress_map[idx] = (
                f"❌ `[{idx}/{total}]` UL failed: `{fname_display}`\n    `{str(e)[:80]}`"
            )
            results["failed"].append(fname)
            with stats_lock:
                stats["total_failed"] += 1
                stats["last_error"] = f"UL {fname}: {str(e)[:100]}"
        finally:
            if split_dir_cleanup:
                shutil.rmtree(split_dir_cleanup, ignore_errors=True)
            if thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except OSError:
                    pass
            shutil.rmtree(file_dir, ignore_errors=True)

# ── Mode selection keyboard ───────────────────────────────────────


def mode_keyboard(remote_path: str, delete: bool = False, size_cap_on: bool = False) -> InlineKeyboardMarkup:
    del_icon = "🗑 Delete ON  ✅" if delete else "🗑 Delete OFF  ❌"
    del_action = "deloff" if delete else "delon"
    size_icon = "📏 Size Cap ON  ✅" if size_cap_on else "📏 Size Cap OFF  ❌"
    size_action = "sizeoff" if size_cap_on else "sizeon"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎬 Video", callback_data=f"mode:video:{remote_path}"),
            InlineKeyboardButton("📄 Document", callback_data=f"mode:doc:{remote_path}"),
        ],
        [
            InlineKeyboardButton(del_icon, callback_data=f"deltoggle:{del_action}:{remote_path}"),
        ],
        [
            InlineKeyboardButton(size_icon, callback_data=f"sizetoggle:{size_action}:{remote_path}"),
        ]
    ])

# ── Shared job runner (used by /dl callback and /retry) ─────────────────────


async def _run_job(status_msg: Message, files: list[str], remote_path: str,
                   mode: str, user_id: int, do_delete: bool,
                   size_map: dict[str, int] | None = None) -> dict:
    """Run a full upload job over `files`. Returns the results dict."""
    global last_failed, last_job_remote, last_job_mode

    total = len(files)
    mode_icon = "🎬 Video" if mode == "video" else "📄 Document"
    del_note = " · 🗑 Delete ON" if do_delete else ""

    # Snapshot the concurrency for this entire job so a live /concurrent
    # change cannot desync the semaphore from the displayed value.
    job_concurrent = CONCURRENT_JOBS

    await safe_edit(
        status_msg,
        f"📦 **{total} file(s)** · ⚡ **{job_concurrent} concurrent**\n"
        f"{'─' * 28}\n"
        f"🔍 Starting {mode_icon} mode{del_note}…"
    )

    active_sessions.add(user_id)
    cancel_flags[user_id] = False
    upload_mode[user_id] = mode
    with stats_lock:
        stats["current_files"] = [f.split("/")[-1] for f in files[:3]]

    semaphore = asyncio.Semaphore(job_concurrent)
    progress_map: dict[int, str] = {}
    results = {"success": 0, "failed": [], "deleted": 0, "delete_failed": [], "skipped": []}
    board_state = _BoardState()

    try:
        tasks = []

        async def _guarded(fname, full_remote, idx):
            if cancel_flags.get(user_id):
                return
            await process_one_file(
                fname, full_remote, idx, total,
                mode, user_id, semaphore,
                progress_map, status_msg, results,
                job_concurrent,
                do_delete=do_delete,
                board_state=board_state,
                size_map=size_map,
            )

        for idx, fname in enumerate(files, 1):
            # Normalise remote_path: strip trailing slash, then fix any
            # accidental leading slash after the colon (e.g. "Dropbox3:/path")
            _rp = remote_path.rstrip("/")
            if ":" in _rp:
                _rpx, _rpp = _rp.split(":", 1)
                _rpp = _rpp.lstrip("/")
                # If path part is empty (e.g. "Dropbox3:"), join without slash
                if _rpp:
                    _rp = _rpx + ":" + _rpp
                else:
                    _rp = _rpx + ":"
            if total == 1 and not remote_path.endswith("/"):
                full_remote = _rp
            elif _rp.endswith(":"):
                full_remote = _rp + fname
            else:
                full_remote = _rp + "/" + fname
            log.info(f"[job] full_remote={full_remote!r}")
            tasks.append(asyncio.create_task(_guarded(fname, full_remote, idx)))

        await board_state.start(status_msg, total, job_concurrent, progress_map)
        await asyncio.gather(*tasks)
    finally:
        await board_state.stop(status_msg, total, job_concurrent, progress_map)
        active_sessions.discard(user_id)
        cancel_flags.pop(user_id, None)
        with stats_lock:
            stats["current_files"] = []

    # Remember failures for /retry
    with state_lock:
        last_failed = list(results["failed"])
        last_job_remote = remote_path
        last_job_mode = mode

    return results | {"total": total, "job_concurrent": job_concurrent,
                      "mode_icon": mode_icon, "do_delete": do_delete}


async def _send_summary(status_msg: Message, results: dict) -> None:
    total = results["total"]
    success = results["success"]
    failed = results["failed"]
    skipped = results.get("skipped", [])
    deleted = results["deleted"]
    delete_failed = results["delete_failed"]
    mode_icon = results["mode_icon"]
    job_concurrent = results["job_concurrent"]
    do_delete = results["do_delete"]

    with stats_lock:
        tbytes = stats["total_bytes"]

    summary = (
        f"🎉 **Job Complete!**\n\n"
        f"✅ **{success}/{total}** files uploaded\n"
        f"📦 **Total:** {fmt_size(tbytes)}\n"
        f"⚡ **Concurrent:** {job_concurrent}\n"
        f"{mode_icon} mode"
    )
    if do_delete:
        summary += f"\n🗑 **Deleted from remote:** {deleted}/{success}"
    if skipped:
        skip_list = "\n".join(f"• `{f}`" for f in skipped[:10])
        if len(skipped) > 10:
            skip_list += f"\n…and {len(skipped) - 10} more"
        summary += f"\n\n⏭ **Skipped — over size limit ({len(skipped)}):**\n{skip_list}"
    if failed:
        fail_list = "\n".join(f"• `{f}`" for f in failed[:10])
        if len(failed) > 10:
            fail_list += f"\n…and {len(failed) - 10} more"
        summary += f"\n\n❌ **Failed ({len(failed)}):**\n{fail_list}"
        summary += "\n\n🔁 Use `/retry` to re-run failed files."
    if delete_failed:
        df_list = "\n".join(f"• `{f}`" for f in delete_failed[:5])
        summary += f"\n\n⚠️ **Delete failed ({len(delete_failed)}):**\n{df_list}"

    await asyncio.sleep(3.0)
    await safe_edit(status_msg, summary)

# ── Commands ──────────────────────────────────────────────────


@app.on_message(filters.command("start") & auth_filter)
async def cmd_start(_, msg: Message):
    await msg.reply(
        "**🤖 Rclone → Telegram Bot**\n\n"
        "**Commands:**\n"
        "`/dl Dropbox:path/` — download & upload all files\n"
        "`/retry` — re-run files that failed in the last job\n"
        "`/setrclone` — upload a new rclone.conf (send file after running this)\n"
        "`/queue` — show current job progress\n"
        "`/setdelete on|off` — auto-delete from remote after upload\n"
        "`/setmaxsize on|off|2000` — limit downloads to N MB (default 2000)\n"
        "`/concurrent 2` — set concurrent jobs (1–5)\n"
        "`/setbwlimit 8M|off` — limit rclone download bandwidth\n"
        "`/status` — bot stats & health\n"
        "`/logs` — last 30 log lines\n"
        "`/cancel` — stop current job gracefully (finishes current files)\n"
        "`/forcestop` — stop the running job immediately\n"
        "`/stop` — shutdown bot\n\n"
        "**Features:**\n"
        f"• ⚡ Up to **{CONCURRENT_JOBS}** files at once\n"
        "• 📊 Live download & upload progress with % and speed\n"
        "• 🎬 Video or 📄 Document mode per job\n"
        "• Auto thumbnail for videos (ffmpeg)\n"
        "• Auto-split files > 1.99 GB\n"
        "• 📏 Optional max download size cap (skips oversized files)\n"
        "• 🗑 Optional auto-delete from remote"
    )


@app.on_message(filters.command("status") & auth_filter)
async def cmd_status(_, msg: Message):
    with stats_lock:
        snap = dict(stats)
    with state_lock:
        del_on = delete_after_upload
        bw = BW_LIMIT or "off"
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
    icon = "🟡" if active_sessions else "🟢"
    cur = snap["current_files"]
    text = (
        f"{icon} **Bot Status**\n\n"
        f"⏱ **Uptime:** `{fmt_uptime()}`\n"
        f"⚡ **Concurrent:** `{CONCURRENT_JOBS}`\n"
        f"🔄 **Active jobs:** `{len(active_sessions)}`\n"
        f"✅ **Files done:** `{snap['total_done']}`\n"
        f"❌ **Files failed:** `{snap['total_failed']}`\n"
        f"📦 **Total uploaded:** `{fmt_size(snap['total_bytes'])}`\n"
        f"🚦 **BW limit:** `{bw}`\n"
    )
    if cur:
        text += f"🔄 **Processing:** `{'`, `'.join(cur[:3])}`\n"
    if snap["last_file"]:
        text += f"📄 **Last done:** `{snap['last_file']}`\n"
    if snap["last_error"]:
        text += f"⚠️ **Last error:** `{snap['last_error'][:100]}`\n"
    del_status = "ON 🟢" if del_on else "OFF 🔴"
    text += f"🗑 **Auto-delete:** `{del_status}`\n"
    size_status = f"ON 🟢 ({cap_mb} MB)" if size_on else "OFF 🔴"
    text += f"📏 **Size cap:** `{size_status}`\n"
    fw_count = snap["floodwait_count"]
    if fw_count > 0:
        fw_secs = snap["floodwait_last_secs"]
        fw_mins = fw_secs // 60
        fw_rem = fw_secs % 60
        fw_fmt = f"{fw_mins}m {fw_rem}s" if fw_mins > 0 else f"{fw_secs}s"
        fw_ago = ""
        if snap["floodwait_last_time"]:
            ago = int(time.time() - snap["floodwait_last_time"])
            ah, ar = divmod(ago, 3600)
            am, as_ = divmod(ar, 60)
            fw_ago = f" (last: {ah}h {am}m {as_}s ago)" if ah > 0 else (
                f" (last: {am}m {as_}s ago)" if am > 0 else f" (last: {as_}s ago)")
        text += f"🚦 **FloodWait:** `{fw_count}x` · last wait `{fw_fmt}`{fw_ago}\n"
    res = _server_resources()
    if res:
        text += f"\n──────────────\n{res}\n"
    text += f"\n🌐 **Health:** port `{HEALTH_PORT}` → `/health`"
    await msg.reply(text)


@app.on_message(filters.command("queue") & auth_filter)
async def cmd_queue(_, msg: Message):
    with stats_lock:
        cur = list(stats["current_files"])
    if not active_sessions:
        await msg.reply("📥 No active job. Use `/dl` to start one.")
        return
    body = "\n".join(f"• `{f}`" for f in cur) if cur else "_warming up…_"
    await msg.reply(f"🔄 **Active job** — currently processing:\n{body}")


@app.on_message(filters.command("logs") & auth_filter)
async def cmd_logs(_, msg: Message):
    lines = list(log_buffer)[-30:]
    if not lines:
        await msg.reply("No logs yet.")
        return
    text = "```\n" + "\n".join(lines) + "\n```"
    if len(text) > 4000:
        text = "```\n" + "\n".join(lines[-15:]) + "\n```"
    await msg.reply(text)


@app.on_message(filters.command("cancel") & auth_filter)
async def cmd_cancel(_, msg: Message):
    user_id = msg.from_user.id
    if user_id in awaiting_rclone_conf:
        awaiting_rclone_conf.discard(user_id)
        await msg.reply("❌ rclone.conf upload cancelled.")
        return
    if user_id not in active_sessions:
        await msg.reply("ℹ️ No active job to cancel.")
        return
    cancel_flags[user_id] = True
    await msg.reply("🛑 Cancel requested — current files will finish then job stops.")


@app.on_message(filters.command("forcestop") & auth_filter)
async def cmd_forcestop(_, msg: Message):
    if not active_sessions:
        await msg.reply("ℹ️ No active job to stop.")
        return
    # Flag all active sessions as cancelled so any in-flight loops bail out.
    for uid in list(active_sessions):
        cancel_flags[uid] = True
    await msg.reply(
        "⛔ **Force-stopping now.**\n"
        "Killing the current job immediately. Partial temp files are "
        "removed on the next start."
    )
    log.warning("Force-stop via /forcestop")
    _persist_stats()
    await asyncio.sleep(1)
    os._exit(0)


@app.on_message(filters.command("stop") & auth_filter)
async def cmd_stop(_, msg: Message):
    await msg.reply("⛔ Shutting down…")
    log.info("Shutdown via /stop")
    _persist_stats()
    await asyncio.sleep(1)
    os._exit(0)


@app.on_message(filters.command("restart") & auth_filter)
async def cmd_restart(_, msg: Message):
    if active_sessions:
        await msg.reply(
            "⚠️ A job is still running — restarting now would interrupt "
            "in-flight downloads and leave temp files behind.\n"
            "Use `/cancel` first, then `/restart` once the job has stopped."
        )
        return
    await msg.reply("🔄 Restarting…")
    log.info("Restart via /restart")
    _persist_stats()
    await asyncio.sleep(1)
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.command("setdelete") & auth_filter)
async def cmd_setdelete(_, msg: Message):
    global delete_after_upload
    if len(msg.command) < 2:
        with state_lock:
            state = "ON 🟢" if delete_after_upload else "OFF 🔴"
        await msg.reply(
            f"🗑 **Auto-delete is currently: {state}**\n\n"
            "Usage:\n"
            "`/setdelete on` — delete file from remote after successful upload\n"
            "`/setdelete off` — keep files on remote (default)"
        )
        return
    arg = msg.command[1].strip().lower()
    if arg == "on":
        with state_lock:
            delete_after_upload = True
        await msg.reply("🗑 **Auto-delete: ON 🟢**\nFiles will be deleted from remote after each successful upload.")
    elif arg == "off":
        with state_lock:
            delete_after_upload = False
        await msg.reply("🗑 **Auto-delete: OFF 🔴**\nFiles will be kept on remote after upload.")
    else:
        await msg.reply("❌ Usage: `/setdelete on` or `/setdelete off`")


@app.on_message(filters.command("setmaxsize") & auth_filter)
async def cmd_setmaxsize(_, msg: Message):
    global max_size_enabled, max_size_bytes
    if len(msg.command) < 2:
        with state_lock:
            state = "ON 🟢" if max_size_enabled else "OFF 🔴"
            cap_mb = max_size_bytes // (1024 * 1024)
        await msg.reply(
            f"📏 **Max file size limit is currently: {state}** (`{cap_mb} MB`)\n\n"
            "Usage:\n"
            "`/setmaxsize on` — enable the limit (uses last/default value)\n"
            "`/setmaxsize off` — disable the limit (download any size)\n"
            "`/setmaxsize 2000` — set limit to 2000 MB and enable it\n\n"
            "Files larger than the limit are skipped before downloading."
        )
        return
    arg = msg.command[1].strip().lower()
    if arg == "on":
        with state_lock:
            max_size_enabled = True
            cap_mb = max_size_bytes // (1024 * 1024)
        await msg.reply(f"📏 **Max file size limit: ON 🟢** (`{cap_mb} MB`)\nLarger files will be skipped.")
    elif arg == "off":
        with state_lock:
            max_size_enabled = False
        await msg.reply("📏 **Max file size limit: OFF 🔴**\nFiles of any size will be downloaded.")
    else:
        try:
            mb = int(arg)
            if mb <= 0:
                raise ValueError
        except ValueError:
            await msg.reply("❌ Usage: `/setmaxsize on`, `/setmaxsize off`, or `/setmaxsize 2000` (MB).")
            return
        with state_lock:
            max_size_bytes = mb * 1024 * 1024
            max_size_enabled = True
        await msg.reply(f"📏 **Max file size limit: ON 🟢** (`{mb} MB`)\nLarger files will be skipped.")


@app.on_message(filters.command("concurrent") & auth_filter)
async def cmd_concurrent(_, msg: Message):
    global CONCURRENT_JOBS
    if len(msg.command) < 2:
        await msg.reply(
            f"⚡ **Concurrent jobs: `{CONCURRENT_JOBS}`**\n\n"
            "Usage: `/concurrent 2`\n"
            "Range: 1–5\n\n"
            "⚠️ For large files (500MB+) use `1` to avoid Telegram rate limits.\n"
            "Note: takes effect on the **next** job, not a running one."
        )
        return
    try:
        val = int(msg.command[1].strip())
        if not 1 <= val <= 5:
            raise ValueError
    except ValueError:
        await msg.reply("❌ Value must be between 1 and 5. Example: `/concurrent 2`")
        return
    CONCURRENT_JOBS = val
    await msg.reply(
        f"⚡ **Concurrent jobs set to: `{CONCURRENT_JOBS}`**\n\n"
        f"{'✅ Good for large files.' if val == 1 else '⚡ Multiple files at once — watch for rate limits.'}"
    )


@app.on_message(filters.command("setbwlimit") & auth_filter)
async def cmd_setbwlimit(_, msg: Message):
    global BW_LIMIT
    if len(msg.command) < 2:
        with state_lock:
            cur = BW_LIMIT or "off"
        await msg.reply(
            f"🚦 **Bandwidth limit: `{cur}`**\n\n"
            "Usage: `/setbwlimit 8M` or `/setbwlimit off`\n"
            "Accepts rclone units, e.g. `512k`, `8M`, `1G`.\n"
            "Applies to the **next** download (not files already in flight)."
        )
        return
    val = msg.command[1].strip()
    if val.lower() == "off":
        with state_lock:
            BW_LIMIT = ""
        await msg.reply("🚦 **Bandwidth limit removed.**")
        return
    if not re.fullmatch(r"\d+(\.\d+)?[kKmMgG]?", val):
        await msg.reply("❌ Invalid value. Examples: `512k`, `8M`, `1G`, or `off`.")
        return
    with state_lock:
        BW_LIMIT = val
    await msg.reply(f"🚦 **Bandwidth limit set to `{val}`** (applies to next download).")


@app.on_message(filters.command("setrefresh") & auth_filter)
async def cmd_setrefresh(_, msg: Message):
    global BOARD_REFRESH_INTERVAL
    if len(msg.command) < 2:
        await msg.reply(
            f"🔄 **Board refresh interval: `{BOARD_REFRESH_INTERVAL:.1f}s`**\n\n"
            "Usage: `/setrefresh 3`\n"
            "Range: 2–30 seconds\n\n"
            "💡 Lower = more frequent edits (watch Telegram rate limits)\n"
            "  `3s` → ~20 edits/min (max safe)\n"
            "  `5s` → ~12 edits/min (default)\n"
            "  `10s` → ~6 edits/min (conservative)"
        )
        return
    try:
        val = float(msg.command[1].strip())
        if not 2.0 <= val <= 30.0:
            raise ValueError
    except ValueError:
        await msg.reply("❌ Value must be between 2 and 30 seconds. Example: `/setrefresh 3`")
        return
    BOARD_REFRESH_INTERVAL = val
    edits_per_min = int(60 / val)
    await msg.reply(
        f"🔄 **Board refresh interval set to: `{BOARD_REFRESH_INTERVAL:.1f}s`**\n"
        f"≈ {edits_per_min} edits/min"
    )


@app.on_message(filters.command("dl") & auth_filter)
async def cmd_dl(_, msg: Message):
    if len(msg.command) < 2:
        await msg.reply("❌ Usage: `/dl Dropbox32:path/to/folder`")
        return
    user_id = msg.from_user.id
    if user_id in active_sessions:
        await msg.reply("⚠️ Job already running. Use `/cancel` first.")
        return
    remote_path = msg.command[1].strip()
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
    size_line = f"📏 Size cap: {'ON 🟢 (' + str(cap_mb) + ' MB)' if size_on else 'OFF 🔴'}"
    await msg.reply(
        f"📂 **Choose upload mode for:**\n`{remote_path}`\n"
        f"🗑 Auto-delete: {'ON 🟢' if del_on else 'OFF 🔴'}\n"
        f"{size_line}",
        reply_markup=mode_keyboard(remote_path, del_on, size_on)
    )


@app.on_message(filters.command("retry") & auth_filter)
async def cmd_retry(_, msg: Message):
    user_id = msg.from_user.id
    if user_id in active_sessions:
        await msg.reply("⚠️ Job already running. Use `/cancel` first.")
        return
    with state_lock:
        files = list(last_failed)
        remote_path = last_job_remote
        mode = last_job_mode
        do_delete = delete_after_upload
    if not files or not remote_path:
        await msg.reply("ℹ️ Nothing to retry — no failed files from the last job.")
        return
    status_msg = await msg.reply(f"🔁 Retrying **{len(files)}** failed file(s)…")
    results = await _run_job(status_msg, files, remote_path, mode, user_id, do_delete)
    await _send_summary(status_msg, results)


def _validate_rclone_conf_text(text: str) -> list[str]:
    """
    Quick sanity check that this looks like an rclone config file.
    Returns the list of remote names found (e.g. ['Dropbox32', 'Gdrive']).
    Raises ValueError if it doesn't look like a valid config.
    """
    remotes = re.findall(r"^\[([^\]]+)\]", text, re.MULTILINE)
    if not remotes:
        raise ValueError("No `[remote]` sections found — this doesn't look like an rclone.conf file.")
    return remotes


@app.on_message(filters.command("setrclone") & auth_filter)
async def cmd_setrclone(_, msg: Message):
    if active_sessions:
        await msg.reply(
            "⚠️ A job is currently running — swapping the rclone config mid-transfer "
            "can break it. Use `/cancel` first, then `/setrclone` again."
        )
        return
    user_id = msg.from_user.id
    awaiting_rclone_conf.add(user_id)
    await msg.reply(
        "📤 **Send your `rclone.conf` file now** as a Telegram document "
        "(just attach the file, no caption needed).\n\n"
        "It will replace the bot's current rclone config. "
        "The old one is kept as a backup.\n\n"
        "Send /cancel to abort."
    )


@app.on_message(filters.document & auth_filter)
async def on_rclone_conf_document(_, msg: Message):
    user_id = msg.from_user.id
    if user_id not in awaiting_rclone_conf:
        return  # not expecting a file from this user right now — ignore silently

    awaiting_rclone_conf.discard(user_id)

    doc = msg.document
    if doc.file_size and doc.file_size > 1024 * 1024:
        await msg.reply("❌ That file is too large to be an rclone.conf. Aborted.")
        return

    status = await msg.reply("⬇️ Downloading config file…")
    tmp_path = os.path.join(DOWNLOAD_DIR, f"rclone_upload_{user_id}_{int(time.time())}.conf")
    try:
        await msg.download(file_name=tmp_path)
    except Exception as e:
        await safe_edit(status, f"❌ Failed to download file:\n`{e}`")
        return

    try:
        text = Path(tmp_path).read_text(encoding="utf-8", errors="strict")
        remotes = _validate_rclone_conf_text(text)
    except UnicodeDecodeError:
        await safe_edit(status, "❌ File isn't valid UTF-8 text — doesn't look like an rclone.conf.")
        Path(tmp_path).unlink(missing_ok=True)
        return
    except ValueError as e:
        await safe_edit(status, f"❌ {e}")
        Path(tmp_path).unlink(missing_ok=True)
        return
    except Exception as e:
        await safe_edit(status, f"❌ Couldn't read file:\n`{e}`")
        Path(tmp_path).unlink(missing_ok=True)
        return

    try:
        RCLONE_CONF_PATH.parent.mkdir(parents=True, exist_ok=True)
        if RCLONE_CONF_PATH.exists():
            backup_path = RCLONE_CONF_PATH.with_suffix(".conf.bak")
            shutil.copy2(RCLONE_CONF_PATH, backup_path)
        RCLONE_CONF_PATH.write_text(text, encoding="utf-8")
        os.environ["RCLONE_CONFIG"] = str(RCLONE_CONF_PATH)
        log.info(f"rclone.conf replaced via /setrclone by user {user_id} ({len(remotes)} remote(s))")
    except Exception as e:
        await safe_edit(status, f"❌ Failed to write rclone.conf:\n`{e}`")
        Path(tmp_path).unlink(missing_ok=True)
        return

    Path(tmp_path).unlink(missing_ok=True)
    remote_list = "\n".join(f"• `{r}`" for r in remotes)
    await safe_edit(
        status,
        f"✅ **rclone.conf updated** — {len(remotes)} remote(s) found:\n{remote_list}\n\n"
        "ℹ️ Previous config backed up to `rclone.conf.bak`.\n"
        "No restart needed — new downloads will use this config."
    )


@app.on_callback_query(auth_filter)
async def cb_mode(_, cq: CallbackQuery):
    global delete_after_upload, max_size_enabled

    if cq.data.startswith("deltoggle:"):
        parts = cq.data.split(":", 2)
        _, action, remote_path = parts
        with state_lock:
            delete_after_upload = (action == "delon")
            del_on = delete_after_upload
            size_on = max_size_enabled
            cap_mb = max_size_bytes // (1024 * 1024)
        del_text = "🗑 Auto-delete: ON 🟢" if del_on else "🗑 Auto-delete: OFF 🔴"
        size_text = f"📏 Size cap: {'ON 🟢 (' + str(cap_mb) + ' MB)' if size_on else 'OFF 🔴'}"
        await cq.answer(del_text)
        await cq.message.edit(
            f"📂 **Choose upload mode for:**\n`{remote_path}`\n{del_text}\n{size_text}",
            reply_markup=mode_keyboard(remote_path, del_on, size_on)
        )
        return

    if cq.data.startswith("sizetoggle:"):
        parts = cq.data.split(":", 2)
        _, action, remote_path = parts
        with state_lock:
            max_size_enabled = (action == "sizeon")
            size_on = max_size_enabled
            cap_mb = max_size_bytes // (1024 * 1024)
            del_on = delete_after_upload
        del_text = "🗑 Auto-delete: ON 🟢" if del_on else "🗑 Auto-delete: OFF 🔴"
        size_text = f"📏 Size cap: {'ON 🟢 (' + str(cap_mb) + ' MB)' if size_on else 'OFF 🔴'}"
        await cq.answer(size_text)
        await cq.message.edit(
            f"📂 **Choose upload mode for:**\n`{remote_path}`\n{del_text}\n{size_text}",
            reply_markup=mode_keyboard(remote_path, del_on, size_on)
        )
        return

    if not cq.data.startswith("mode:"):
        return
    parts = cq.data.split(":", 2)
    if len(parts) < 3:
        return
    _, chosen, remote_path = parts
    mode = "video" if chosen == "video" else "document"
    mode_icon = "🎬 Video" if mode == "video" else "📄 Document"
    user_id = cq.from_user.id
    if user_id in active_sessions:
        await cq.answer("A job is already running.", show_alert=True)
        return
    with state_lock:
        do_delete = delete_after_upload
        size_cap_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
    del_note = " · 🗑 Delete ON" if do_delete else ""
    size_note = f" · 📏 Cap {cap_mb}MB" if size_cap_on else ""

    await cq.message.edit(f"📂 `{remote_path}`\n{mode_icon} mode{del_note}{size_note}\n\n🔍 Listing files…")

    try:
        files = await asyncio.get_running_loop().run_in_executor(
            None, rclone_list, remote_path)
    except Exception as e:
        with stats_lock:
            stats["last_error"] = str(e)[:200]
        await cq.message.edit(f"❌ Failed to list files:\n`{e}`")
        return

    if not files:
        await cq.message.edit("❌ No files found at that path.")
        return

    size_map: dict[str, int] | None = None
    if size_cap_on:
        size_map = await asyncio.get_running_loop().run_in_executor(
            None, rclone_size_map, remote_path)

    status_msg = cq.message
    results = await _run_job(status_msg, files, remote_path, mode, user_id, do_delete,
                             size_map=size_map)
    await _send_summary(status_msg, results)

# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    if psutil is not None:
        # Prime the CPU sampler so the first /status reports a real value
        # instead of 0.0 (cpu_percent(interval=None) is relative to last call).
        try:
            psutil.cpu_percent(interval=None)
        except Exception:
            pass
    threading.Thread(target=_run_health, daemon=True).start()
    log.info(f"Health check on port {HEALTH_PORT}")
    log.info(f"Concurrent jobs: {CONCURRENT_JOBS}")
    log.info("Bot starting…")
    app.run()
