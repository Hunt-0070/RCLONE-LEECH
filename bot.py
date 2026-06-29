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

# Install motor if missing: pip install motor pymongo
try:
    from motor.motor_asyncio import AsyncIOMotorClient
except ImportError:
    AsyncIOMotorClient = None

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
MONGO_URI = os.environ.get("MONGO_URI", "").strip()

if _MISSING:
    for k in _MISSING:
        log.critical(f"Missing required variable: {k}")
    raise SystemExit(1)

API_ID = int(_API_ID)
OWNER_ID = int(_OWNER)
DUMP_CHAT_ID = int(_DUMP)

# Multi-user authorization
AUTHORIZED_USERS: set[int] = {OWNER_ID}
for _uid in os.environ.get("AUTHORIZED_USERS", "").replace(";", ",").split(","):
    _uid = _uid.strip()
    if _uid:
        try:
            AUTHORIZED_USERS.add(int(_uid))
        except ValueError:
            log.warning(f"Ignoring invalid AUTHORIZED_USERS entry: {_uid}")

# ── Optional mutable variables ──────────────────────────────────
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/tmp/rclone_dl").strip()
RCLONE_FLAGS = os.environ.get("RCLONE_FLAGS", "").strip()
SPLIT_SIZE = int(os.environ.get("SPLIT_SIZE", str(2000 * 1024 * 1024)))
HEALTH_PORT = int(os.environ.get("PORT") or os.environ.get("HEALTH_PORT") or 8080)
CONCURRENT_JOBS = max(1, min(5, int(os.environ.get("CONCURRENT_JOBS", 1))))
BW_LIMIT = os.environ.get("BW_LIMIT", "").strip()
STATS_FILE = os.environ.get("STATS_FILE", "/tmp/rclone_bot_stats.json").strip()
cores = os.environ.get("CPU_CORES", "0")
threads = int(os.environ.get("CPU_THREADS", os.cpu_count() or 1))

Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

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
awaiting_rclone_conf: set[int] = set()
delete_after_upload: bool = False
max_size_enabled: bool = False
max_size_bytes: int = 2000 * 1024 * 1024
last_failed: list[str] = []
last_job_remote: str | None = None
last_job_mode: str = "video"
state_lock = threading.Lock()
BOARD_REFRESH_INTERVAL = float(os.environ.get("BOARD_REFRESH_INTERVAL", "8.0"))
auto_start_loop: bool = False  # Controlled by /autostart command

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

# ── MongoDB Integration Logic ─────────────────────────────────
db = None
if AsyncIOMotorClient and MONGO_URI:
    try:
        mongo_client = AsyncIOMotorClient(MONGO_URI)
        db = mongo_client["rclone_telegram_bot"]
        log.info("MongoDB client loaded successfully.")
    except Exception as mongo_err:
        log.error(f"MongoDB connection configuration initialization failed: {mongo_err}")
else:
    log.warning("MongoDB client or MONGO_URI missing. Persistent DB features disabled.")


async def save_config_to_mongo():
    """Serializes the configuration settings into the remote database."""
    if db is None:
        return
    try:
        with state_lock:
            config_doc = {
                "_id": "bot_settings",
                "concurrent_jobs": CONCURRENT_JOBS,
                "bw_limit": BW_LIMIT,
                "delete_after_upload": delete_after_upload,
                "max_size_enabled": max_size_enabled,
                "max_size_bytes": max_size_bytes,
                "board_refresh_interval": BOARD_REFRESH_INTERVAL,
                "auto_start_loop": auto_start_loop,
                "last_job_remote": last_job_remote,
                "last_job_mode": last_job_mode,
                "last_failed": last_failed
            }
        await db["settings"].replace_one({"_id": "bot_settings"}, config_doc, upsert=True)
        log.info("Configuration successfully pushed to MongoDB.")
    except Exception as e:
        log.error(f"Failed writing configuration data to MongoDB layer: {e}")


async def load_config_from_mongo():
    """Fetches configuration structures and sets global states at startup."""
    global CONCURRENT_JOBS, BW_LIMIT, delete_after_upload, max_size_enabled, max_size_bytes, BOARD_REFRESH_INTERVAL, auto_start_loop, last_job_remote, last_job_mode, last_failed
    if db is None:
        return
    try:
        doc = await db["settings"].find_one({"_id": "bot_settings"})
        if doc:
            with state_lock:
                CONCURRENT_JOBS = doc.get("concurrent_jobs", CONCURRENT_JOBS)
                BW_LIMIT = doc.get("bw_limit", BW_LIMIT)
                delete_after_upload = doc.get("delete_after_upload", delete_after_upload)
                max_size_enabled = doc.get("max_size_enabled", max_size_enabled)
                max_size_bytes = doc.get("max_size_bytes", max_size_bytes)
                BOARD_REFRESH_INTERVAL = doc.get("board_refresh_interval", BOARD_REFRESH_INTERVAL)
                auto_start_loop = doc.get("auto_start_loop", auto_start_loop)
                last_job_remote = doc.get("last_job_remote", last_job_remote)
                last_job_mode = doc.get("last_job_mode", last_job_mode)
                last_failed = doc.get("last_failed", last_failed)
            log.info("Configuration parameters synced up cleanly from MongoDB mapping profile.")
    except Exception as e:
        log.error(f"Failed extraction sequence parsing MongoDB properties: {e}")


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
            return
        except Exception as e:
            log.error(f"Failed to write rclone.conf: {e}")


_write_rclone_conf()

def _persist_stats() -> None:
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
            for k in ("total_done", "total_failed", "total_bytes", "floodwait_count"):
                if isinstance(loaded.get(k), int):
                    stats[k] = loaded[k]
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
    lines = []
    if psutil is not None:
        try:
            cpu = psutil.cpu_percent(interval=None)
            cores_count = psutil.cpu_count(logical=True) or 1
            vm = psutil.virtual_memory()
            lines.append(f"🖥 **CPU:** `{cpu:.0f}%` over `{cores_count}` core(s)")
            lines.append(f"🧠 **RAM:** `{fmt_size(vm.used)} / {fmt_size(vm.total)}` (`{vm.percent:.0f}%`)")
        except Exception as e:
            log.debug(f"_server_resources psutil failed: {e}")
    else:
        lines.append("🖥 **CPU/RAM:** `psutil not installed`")

    try:
        du = shutil.disk_usage(DOWNLOAD_DIR)
        pct = du.used * 100 / du.total if du.total else 0
        lines.append(f"💾 **Disk:** `{fmt_size(du.used)} / {fmt_size(du.total)}` (`{pct:.0f}%`, free `{fmt_size(du.free)}`)")
    except Exception as e:
        log.debug(f"_server_resources disk failed: {e}")

    return "\n".join(lines)


def fmt_eta(seconds: float) -> str:
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
    if total <= 0:
        return "○" * width
    pct = max(0.0, min(1.0, done / total))
    filled = round(width * pct)
    filled = max(0, min(width, filled))
    return "●" * filled + "○" * (width - filled)


def make_progress_line(label: str, done_bytes: int, total_bytes: int, speed_str: str = "", eta_str: str = "", width: int = 12, elapsed_str: str = "", file_idx: str = "") -> str:
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

    lines[-1] = lines[-1].replace("├", "└", 1)
    return "\n".join(lines)

# ── rclone progress parser ───────────────────────────────────────
_RCLONE_STATS_RE = re.compile(r"(?:Transferred:\s+)?([\d.]+\s*\S+)\s*/\s*([\d.]+\s*\S+),\s*(\d+)%,\s*([\d.]+\s*\S+/s)(?:,\s*ETA\s*(\S+))?", re.IGNORECASE)


def _parse_rclone_line(line: str) -> dict | None:
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
    units = {"b": 1, "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3, "tib": 1024 ** 4, "kb": 1000, "mb": 1000 ** 2, "gb": 1000 ** 3}
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
    return [
        f.strip().lstrip("/") for f in r.stdout.splitlines()
        if f.strip() and f.strip().rsplit(".", 1)[-1].lower() in allowed
    ]


def rclone_size_map(remote_path: str) -> dict[str, int]:
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


def rclone_download(remote_path: str, dest_dir: str, progress_callback=None) -> str:
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
        if parsed and progress_callback:
            now = time.monotonic()
            if last_progress_call == 0.0 or now - last_progress_call >= 5.0:
                last_progress_call = now
                done_bytes = _parse_size_str(parsed["done_str"])
                total_bytes = _parse_size_str(parsed["total_str"])
                progress_callback(done_bytes, total_bytes, parsed["pct"], parsed["speed"], parsed["eta"])

    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(last_line or "rclone copyto failed")
    return local_dest


def rclone_delete(remote_path: str) -> None:
    cmd = ["rclone", "deletefile", remote_path]
    r = subprocess.run(cmd, capture_output=True, text=True, env=_rclone_env())
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "rclone deletefile failed")


def split_file(local_path: str, split_dir: str, user_id: int = 0) -> list[str]:
    size = os.path.getsize(local_path)
    if size <= SPLIT_SIZE:
        return [local_path]

    split_dir_path = Path(split_dir)
    split_dir_path.mkdir(parents=True, exist_ok=True)
    filename = os.path.basename(local_path)
    stem, dot_ext = os.path.splitext(filename)
    extension = dot_ext

    probe = subprocess.run(["ffprobe", "-hide_banner", "-loglevel", "error", "-print_format", "json", "-show_format", local_path], capture_output=True, text=True)
    duration = 0
    try:
        fields = json.loads(probe.stdout).get("format", {})
        duration = round(float(fields.get("duration", 0)))
    except Exception:
        duration = 0

    if duration == 0:
        return [local_path]

    split_size = SPLIT_SIZE - 3_000_000
    start_time = 0
    i = 1
    parts = (size // SPLIT_SIZE) + 1
    multi_streams = True
    out_paths: list[str] = []

    while i <= parts or start_time < duration - 4:
        if cancel_flags.get(user_id):
            return out_paths or [local_path]
        out_path = str(split_dir_path / f"{stem}.part{i:03}{extension}")

        prefix = ["taskset", "-c", f"{cores}"] if cores != "0" else []
        head = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-ss", str(start_time), "-i", local_path, "-fs", str(split_size)]
        maps = ["-map", "0"] if multi_streams else []
        tail = ["-map_chapters", "-1", "-async", "1", "-strict", "-2", "-c", "copy", "-threads", f"{threads}", out_path]
        cmd = prefix + head + maps + tail

        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            try:
                os.remove(out_path)
            except OSError:
                pass
            if multi_streams:
                multi_streams = False
                continue
            else:
                return [local_path]

        out_size = os.path.getsize(out_path)
        if out_size > SPLIT_SIZE:
            new_split_size = split_size - (out_size - SPLIT_SIZE) - 5_000_000
            if new_split_size < 50_000_000:
                os.remove(out_path)
                return [local_path]
            split_size = new_split_size
            os.remove(out_path)
            continue

        probe2 = subprocess.run(["ffprobe", "-hide_banner", "-loglevel", "error", "-print_format", "json", "-show_format", out_path], capture_output=True, text=True)
        lpd = 0
        try:
            fields2 = json.loads(probe2.stdout).get("format", {})
            lpd = round(float(fields2.get("duration", 0)))
        except Exception:
            lpd = 0

        if lpd == 0:
            break
        elif duration == lpd:
            out_paths.append(out_path)
            break
        elif lpd <= 3:
            os.remove(out_path)
            break

        out_paths.append(out_path)
        start_time += lpd - 3
        i += 1

    if not out_paths:
        return [local_path]
    return out_paths


async def generate_thumbnail(local_path: str) -> str | None:
    thumb_path = local_path + "_thumb.jpg"
    proc = None
    try:
        probe = await asyncio.create_subprocess_exec("ffprobe", "-hide_banner", "-loglevel", "error", "-print_format", "json", "-show_format", local_path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        stdout, _ = await asyncio.wait_for(probe.communicate(), timeout=15)
        seek_time = 3
        try:
            duration = float(json.loads(stdout).get("format", {}).get("duration", 0))
            if duration > 0:
                seek_time = max(1, int(duration / 2))
        except Exception:
            pass

        proc = await asyncio.create_subprocess_exec("ffmpeg", "-hide_banner", "-loglevel", "error", "-ss", str(seek_time), "-i", local_path, "-vf", "thumbnail", "-q:v", "1", "-frames:v", "1", "-threads", "1", thumb_path, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await asyncio.wait_for(proc.wait(), timeout=30)
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            return thumb_path
    except Exception:
        if proc and proc.returncode is None:
            try:
                proc.kill()
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
            with stats_lock:
                stats["floodwait_count"] += 1
                stats["floodwait_last_secs"] = e.value
                stats["floodwait_last_time"] = time.time()
            if attempts >= max_retries:
                raise
            await asyncio.sleep(wait)

# ── Build composite progress statuses ────────────────────────────────
def _render_board(total_files: int, concurrent: int, progress_map: dict) -> str:
    header = f"📦 **{total_files} file(s)** · ⚡ **{concurrent} concurrent**\n"
    separator = "━" * 24

    done = sum(1 for v in progress_map.values() if v.startswith(("✅ ", "✅🗑", "✅⚠️")))
    failed = sum(1 for v in progress_map.values() if v.startswith("❌"))
    skipped = sum(1 for v in progress_map.values() if v.startswith("⏭"))

    active_entries = {k: v for k, v in progress_map.items() if not (v.startswith(("✅ ", "✅🗑", "✅⚠️")) or v.startswith("❌") or v.startswith("⏭"))}

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
        except Exception:
            pass

    try:
        du = shutil.disk_usage(DOWNLOAD_DIR)
        footer_lines.append(f"├ FREE: {fmt_size(du.free)} | {fmt_size(du.total)}")
    except Exception:
        pass

    footer_lines[-1] = footer_lines[-1].replace("├", "└", 1)
    text = header + "\n" + body + "\n\n" + separator + "\n" + "\n".join(footer_lines)
    if len(text) > 4000:
        text = text[:3950] + "\n…"
    return text


class _BoardState:
    def __init__(self):
        self.dirty = False
        self.running = False
        self._task: asyncio.Task | None = None

    def mark_dirty(self):
        self.dirty = True

    async def start(self, status_msg: Message, total_files: int, concurrent: int, progress_map: dict):
        self.running = True
        self.dirty = True
        self._task = asyncio.create_task(self._loop(status_msg, total_files, concurrent, progress_map))

    async def stop(self, status_msg: Message, total_files: int, concurrent: int, progress_map: dict):
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

    async def _loop(self, status_msg: Message, total_files: int, concurrent: int, progress_map: dict):
        while self.running:
            await asyncio.sleep(BOARD_REFRESH_INTERVAL)
            if self.dirty and self.running:
                self.dirty = False
                text = _render_board(total_files, concurrent, progress_map)
                await safe_edit(status_msg, text)


async def _push_board(status_msg: Message, total_files: int, concurrent: int, progress_map: dict, board_state: "_BoardState | None" = None) -> None:
    if board_state is not None:
        board_state.mark_dirty()
    else:
        text = _render_board(total_files, concurrent, progress_map)
        await safe_edit(status_msg, text)


async def upload_part_with_progress(part_path: str, caption: str, mode: str, thumb: str | None, idx: int, total_files: int, fname: str, part_idx: int, total_parts: int, file_size: int, progress_map: dict, status_msg: Message, reply_to_message_id: int | None = None, board_state: "_BoardState | None" = None) -> Message:
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
            lines = [base_label, f"├ [{bar2}] » {pct2}%", f"├ Part {part_idx}/{total_parts}", f"├ Processed: {fmt_size(current)}", f"├ Total Size: {fmt_size(total)}"]
            if speed_str: lines.append(f"├ Speed: {speed_str}")
            if eta_str and eta_str != "—": lines.append(f"├ ETA: {eta_str}")
            lines.append(f"├ Elapsed: {fmt_eta(elapsed)}")
            lines.append(f"├ File: {idx}/{total_files}")
            lines[-1] = lines[-1].replace("├", "└", 1)
            progress_map[idx] = "\n".join(lines)
        else:
            progress_map[idx] = make_progress_line(base_label, current, total, speed_str, eta_str, elapsed_str=fmt_eta(elapsed), file_idx=f"{idx}/{total_files}")
        if board_state is not None:
            board_state.mark_dirty()

    progress_map[idx] = make_progress_line(_make_label(), 0, file_size, file_idx=f"{idx}/{total_files}")
    if board_state is not None:
        board_state.mark_dirty()

    upload_start = [loop.time()]
    is_split = total_parts > 1
    ext = os.path.basename(part_path).rsplit(".", 1)[-1].lower()

    if mode == "video" and (ext in VIDEO_EXTS or is_split):
        return await _send_with_retry(app.send_video, DUMP_CHAT_ID, part_path, caption=caption, supports_streaming=True, thumb=thumb, reply_to_message_id=reply_to_message_id, progress=progress_handler)
    elif ext in AUDIO_EXTS and mode != "document":
        return await _send_with_retry(app.send_audio, DUMP_CHAT_ID, part_path, caption=caption, thumb=thumb, reply_to_message_id=reply_to_message_id, progress=progress_handler)
    elif ext in IMAGE_EXTS and mode != "document":
        return await _send_with_retry(app.send_photo, DUMP_CHAT_ID, part_path, caption=caption, reply_to_message_id=reply_to_message_id, progress=progress_handler)
    else:
        return await _send_with_retry(app.send_document, DUMP_CHAT_ID, part_path, caption=caption, thumb=thumb, reply_to_message_id=reply_to_message_id, progress=progress_handler)


async def process_one_file(fname: str, full_remote: str, idx: int, total: int, mode: str, user_id: int, semaphore: asyncio.Semaphore, progress_map: dict, status_msg: Message, results: dict, job_concurrent: int, do_delete: bool = False, board_state: "_BoardState | None" = None, size_map: dict[str, int] | None = None) -> None:
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
            progress_map[idx] = make_progress_line(f"⬇️ `[{idx}/{total}]` `{fname_display}`", done_bytes, total_bytes, speed_str=speed, eta_str=eta, elapsed_str=fmt_eta(time.monotonic() - dl_start), file_idx=f"{idx}/{total}")
            if board_state is not None:
                board_state.mark_dirty()

        progress_map[idx] = f"⬇️ `[{idx}/{total}]` Connecting to `{fname_display}`…"
        await _push_board(status_msg, total, job_concurrent, progress_map, board_state=board_state)

        with state_lock:
            size_cap_on = max_size_enabled
            size_cap = max_size_bytes
        if size_cap_on:
            remote_size = size_map.get(fname, -1) if size_map else -1
            if remote_size < 0:
                remote_size = await loop.run_in_executor(None, rclone_size, full_remote)
            if remote_size >= 0 and remote_size > size_cap:
                progress_map[idx] = f"⏭ `[{idx}/{total}]` Skipped: `{fname_display}`\n    `{fmt_size(remote_size)} exceeds limit`"
                results["skipped"].append(fname)
                shutil.rmtree(file_dir, ignore_errors=True)
                return

        try:
            local_path = await loop.run_in_executor(None, lambda: rclone_download(full_remote, file_dir, dl_progress))
        except Exception as e:
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

        progress_map[idx] = f"📥 Downloaded ✅ `[{idx}/{total}]` `{fname_display}`\n└ [{'●' * 12}] » 100%  {fmt_size(file_size)}" + (f"  ⚡ avg {dl_speed}" if dl_speed else "")
        await _push_board(status_msg, total, job_concurrent, progress_map, board_state=board_state)

        needs_split = file_size > SPLIT_SIZE
        if ext in VIDEO_EXTS:
            progress_map[idx] = f"🖼 `[{idx}/{total}]` Generating thumbnail for `{fname_display}`…"
            await _push_board(status_msg, total, job_concurrent, progress_map, board_state=board_state)
            thumb_path = await generate_thumbnail(local_path)

        if needs_split:
            progress_map[idx] = f"✂️ `[{idx}/{total}]` Splitting `{fname_display}` ({fmt_size(file_size)})"
            await _push_board(status_msg, total, job_concurrent, progress_map, board_state=board_state)
            split_dir = local_path + "_parts"
            split_dir_cleanup = split_dir
            parts = await loop.run_in_executor(None, split_file, local_path, split_dir, user_id)
            if local_path not in parts:
                try: os.remove(local_path)
                except OSError: pass
        else:
            parts = [local_path]

        total_parts = len(parts)
        try:
            prev_msg_id: int | None = None
            all_parts_sent = True
            for part_idx, part_path in enumerate(parts, 1):
                part_size = os.path.getsize(part_path)
                fname_only = fname.split("/")[-1]
                if ":" in fname_only: fname_only = fname_only.split(":", 1)[-1]
                part_filename = os.path.basename(part_path)
                if ":" in part_filename: part_filename = part_filename.split(":", 1)[-1]
                caption = f"`{part_filename}`\n\n🧩 Part {part_idx}/{total_parts} · {fmt_size(part_size)} | Total: {fmt_size(file_size)}" if total_parts > 1 else f"`{fname_only}`"

                sent = await upload_part_with_progress(part_path, caption, mode, thumb_path, idx, total, fname, part_idx, total_parts, part_size, progress_map, status_msg, prev_msg_id, board_state)
                if sent:
                    prev_msg_id = sent.id
                    try: os.remove(part_path)
                    except OSError: pass
                else:
                    all_parts_sent = False
                with stats_lock:
                    stats["total_bytes"] += part_size

            if do_delete and not all_parts_sent:
                progress_map[idx] = f"✅⚠️ `[{idx}/{total}]` Done (del skipped, upload incomplete): `{fname}`"
                results["delete_failed"].append(fname)
            elif do_delete:
                progress_map[idx] = f"🗑 `[{idx}/{total}]` Deleting from remote…"
                await _push_board(status_msg, total, job_concurrent, progress_map, board_state=board_state)
                try:
                    await loop.run_in_executor(None, rclone_delete, full_remote)
                    progress_map[idx] = f"✅🗑 `[{idx}/{total}]` Done + deleted: `{fname}` · {fmt_size(file_size)}"
                    results["deleted"] += 1
                except Exception as de:
                    progress_map[idx] = f"✅⚠️ `[{idx}/{total}]` Done (del failed): `{fname}`"
                    results["delete_failed"].append(fname)
            else:
                progress_map[idx] = f"✅ `[{idx}/{total}]` `{fname_display}` · {fmt_size(file_size)}"

            results["success"] += 1
            with stats_lock:
                stats["total_done"] += 1
                stats["last_file"] = fname
            _persist_stats()
        except Exception as e:
            progress_map[idx] = f"❌ `[{idx}/{total}]` UL failed: `{fname_display}`\n    `{str(e)[:80]}`"
            results["failed"].append(fname)
            with stats_lock:
                stats["total_failed"] += 1
                stats["last_error"] = f"UL {fname}: {str(e)[:100]}"
        finally:
            if split_dir_cleanup: shutil.rmtree(split_dir_cleanup, ignore_errors=True)
            if thumb_path and os.path.exists(thumb_path):
                try: os.remove(thumb_path)
                except OSError: pass
            shutil.rmtree(file_dir, ignore_errors=True)


def mode_keyboard(remote_path: str, delete: bool = False, size_cap_on: bool = False) -> InlineKeyboardMarkup:
    del_icon = "🗑 Delete ON  ✅" if delete else "🗑 Delete OFF  ❌"
    del_action = "deloff" if delete else "delon"
    size_icon = "📏 Size Cap ON  ✅" if size_cap_on else "📏 Size Cap OFF  ❌"
    size_action = "sizeoff" if size_cap_on else "sizeon"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Video", callback_data=f"mode:video:{remote_path}"), InlineKeyboardButton("📄 Document", callback_data=f"mode:doc:{remote_path}")],
        [InlineKeyboardButton(del_icon, callback_data=f"deltoggle:{del_action}:{remote_path}")],
        [InlineKeyboardButton(size_icon, callback_data=f"sizetoggle:{size_action}:{remote_path}")]
    ])


async def _run_job(status_msg: Message, files: list[str], remote_path: str, mode: str, user_id: int, do_delete: bool, size_map: dict[str, int] | None = None) -> dict:
    global last_failed, last_job_remote, last_job_mode
    total = len(files)
    mode_icon = "🎬 Video" if mode == "video" else "📄 Document"
    del_note = " · 🗑 Delete ON" if do_delete else ""
    job_concurrent = CONCURRENT_JOBS

    await safe_edit(status_msg, f"📦 **{total} file(s)** · ⚡ **{job_concurrent} concurrent**\n{'─' * 28}\n🔍 Starting {mode_icon} mode{del_note}…")
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
            if cancel_flags.get(user_id): return
            await process_one_file(fname, full_remote, idx, total, mode, user_id, semaphore, progress_map, status_msg, results, job_concurrent, do_delete=do_delete, board_state=board_state, size_map=size_map)

        for idx, fname in enumerate(files, 1):
            _rp = remote_path.rstrip("/")
            if ":" in _rp:
                _rpx, _rpp = _rp.split(":", 1)
                _rpp = _rpp.lstrip("/")
                _rp = _rpx + ":" + _rpp if _rpp else _rpx + ":"
            full_remote = _rp if (total == 1 and not remote_path.endswith("/")) else (_rp + fname if _rp.endswith(":") else _rp + "/" + fname)
            tasks.append(asyncio.create_task(_guarded(fname, full_remote, idx)))

        await board_state.start(status_msg, total, job_concurrent, progress_map)
        await asyncio.gather(*tasks)
    finally:
        await board_state.stop(status_msg, total, job_concurrent, progress_map)
        active_sessions.discard(user_id)
        cancel_flags.pop(user_id, None)
        with stats_lock:
            stats["current_files"] = []

    with state_lock:
        last_failed = list(results["failed"])
        last_job_remote = remote_path
        last_job_mode = mode
    await save_config_to_mongo()
    return results | {"total": total, "job_concurrent": job_concurrent, "mode_icon": mode_icon, "do_delete": do_delete}


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

    with stats_lock: tbytes = stats["total_bytes"]
    summary = f"🎉 **Job Complete!**\n\n✅ **{success}/{total}** files uploaded\n📦 **Total:** {fmt_size(tbytes)}\n⚡ **Concurrent:** {job_concurrent}\n{mode_icon} mode"
    if do_delete: summary += f"\n🗑 **Deleted from remote:** {deleted}/{success}"
    if skipped: summary += f"\n\n¼ **Skipped — over size limit:** {len(skipped)}"
    if failed: summary += f"\n\n❌ **Failed ({len(failed)}):**\n" + "\n".join(f"• `{f}`" for f in failed[:10]) + "\n\n🔁 Use `/retry` to re-run failed files."
    await asyncio.sleep(3.0)
    await safe_edit(status_msg, summary)

# ── Background Guardian Loop ──────────────────────────────────
async def task_check_loop():
    """Wakes up every 30 minutes to check if processing halted and requires a system kickstart."""
    while True:
        await asyncio.sleep(1800)  # Check every 30 minutes
        if not auto_start_loop:
            continue

        # If there are files marked as failed and no transfers are active, trigger a retry
        if not active_sessions and last_failed and last_job_remote:
            log.info("Task checking engine detected failure states idling. Restarting last failed transfers...")
            try:
                status_msg = await app.send_message(chat_id=OWNER_ID, text="⚙️ **Auto-Start Loop:** Active jobs stopped with failed items. Re-triggering sequence...")
                results = await _run_job(status_msg, list(last_failed), last_job_remote, last_job_mode, OWNER_ID, delete_after_upload)
                await _send_summary(status_msg, results)
            except Exception as loop_ex:
                log.error(f"Auto-Start loop failed to launch recovery sequence: {loop_ex}")

# ── Commands ──────────────────────────────────────────────────
@app.on_message(filters.command("start") & auth_filter)
async def cmd_start(_, msg: Message):
    await msg.reply(
        "**🤖 Rclone → Telegram Bot**\n\n"
        "**Commands:**\n"
        "`/dl Dropbox:path/` — download & upload all files\n"
        "`/retry` — re-run files that failed in the last job\n"
        "`/autostart on|off` — toggle 30-min task check engine\n"
        "`/setrclone` — upload a new rclone.conf\n"
        "`/queue` — show current job progress\n"
        "`/setdelete on|off` — auto-delete from remote\n"
        "`/setmaxsize on|off|2000` — limit download size (MB)\n"
        "`/concurrent 1-5` — concurrent operations\n"
        "`/status` — bot stats & database tracking\n"
        "`/cancel` — stop current job gracefully"
    )


@app.on_message(filters.command("autostart") & auth_filter)
async def cmd_autostart(_, msg: Message):
    global auto_start_loop
    if len(msg.command) < 2:
        state = "ON 🟢" if auto_start_loop else "OFF 🔴"
        await msg.reply(f"🔁 **Auto-Start engine check is currently: {state}**\nUse `/autostart on` or `/autostart off` to configure.")
        return
    arg = msg.command[1].strip().lower()
    if arg == "on":
        auto_start_loop = True
        await msg.reply("🔁 **Auto-Start Engine: ON 🟢**\nEvery 30 minutes, if active jobs are dead and failed files exist, it will auto-restart.")
    elif arg == "off":
        auto_start_loop = False
        await msg.reply("🔁 **Auto-Start Engine: OFF 🔴**\nIdle states will not be automatically restarted.")
    else:
        await msg.reply("❌ Usage: `/autostart on` or `/autostart off`")
    await save_config_to_mongo()


@app.on_message(filters.command("status") & auth_filter)
async def cmd_status(_, msg: Message):
    with stats_lock: snap = dict(stats)
    with state_lock:
        del_on = delete_after_upload
        bw = BW_LIMIT or "off"
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
    icon = "🟡" if active_sessions else "🟢"
    text = (
        f"{icon} **Bot Status**\n\n"
        f"⏱ **Uptime:** `{fmt_uptime()}`\n"
        f"⚡ **Concurrent:** `{CONCURRENT_JOBS}`\n"
        f"🔄 **Active jobs:** `{len(active_sessions)}`\n"
        f"✅ **Files done:** `{snap['total_done']}`\n"
        f"❌ **Files failed:** `{snap['total_failed']}`\n"
        f"📦 **Total uploaded:** `{fmt_size(snap['total_bytes'])}`\n"
        f"🚦 **BW limit:** `{bw}`\n"
        f"🗑 **Auto-delete:** `{'ON 🟢' if del_on else 'OFF 🔴'}`\n"
        f"📏 **Size cap:** `{f'ON 🟢 ({cap_mb} MB)' if size_on else 'OFF 🔴'}`\n"
        f"🔁 **Auto-Start Check:** `{'ON 🟢' if auto_start_loop else 'OFF 🔴'}`\n"
    )
    res = _server_resources()
    if res: text += f"\n──────────────\n{res}\n"
    await msg.reply(text)


@app.on_message(filters.command("queue") & auth_filter)
async def cmd_queue(_, msg: Message):
    with stats_lock: cur = list(stats["current_files"])
    if not active_sessions:
        await msg.reply("📥 No active job. Use `/dl` to start one.")
        return
    body = "\n".join(f"• `{f}`" for f in cur) if cur else "_warming up…_"
    await msg.reply(f"🔄 **Active job** — currently processing:\n{body}")


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
    for uid in list(active_sessions): cancel_flags[uid] = True
    await msg.reply("⛔ **Force-stopping now.**")
    _persist_stats()
    await asyncio.sleep(1)
    os._exit(0)


@app.on_message(filters.command("restart") & auth_filter)
async def cmd_restart(_, msg: Message):
    if active_sessions:
        await msg.reply("⚠️ A job is still running. Use `/cancel` first.")
        return
    await msg.reply("🔄 Restarting…")
    _persist_stats()
    await asyncio.sleep(1)
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.command("setdelete") & auth_filter)
async def cmd_setdelete(_, msg: Message):
    global delete_after_upload
    if len(msg.command) < 2:
        state = "ON 🟢" if delete_after_upload else "OFF 🔴"
        await msg.reply(f"🗑 **Auto-delete is currently: {state}**\nUse `/setdelete on` or `/setdelete off`.")
        return
    arg = msg.command[1].strip().lower()
    if arg == "on":
        with state_lock: delete_after_upload = True
        await msg.reply("🗑 **Auto-delete: ON 🟢**")
    elif arg == "off":
        with state_lock: delete_after_upload = False
        await msg.reply("🗑 **Auto-delete: OFF 🔴**")
    await save_config_to_mongo()


@app.on_message(filters.command("setmaxsize") & auth_filter)
async def cmd_setmaxsize(_, msg: Message):
    global max_size_enabled, max_size_bytes
    if len(msg.command) < 2:
        state = "ON 🟢" if max_size_enabled else "OFF 🔴"
        cap_mb = max_size_bytes // (1024 * 1024)
        await msg.reply(f"📏 **Max file size limit is currently: {state}** (`{cap_mb} MB`)")
        return
    arg = msg.command[1].strip().lower()
    if arg == "on":
        with state_lock: max_size_enabled = True
        await msg.reply("📏 **Max file size limit: ON 🟢**")
    elif arg == "off":
        with state_lock: max_size_enabled = False
        await msg.reply("📏 **Max file size limit: OFF 🔴**")
    else:
        try:
            mb = int(arg)
            with state_lock:
                max_size_bytes = mb * 1024 * 1024
                max_size_enabled = True
            await msg.reply(f"📏 **Max file size limit: ON 🟢** (`{mb} MB`)")
        except ValueError:
            await msg.reply("❌ Usage: `/setmaxsize on|off|value`")
            return
    await save_config_to_mongo()


@app.on_message(filters.command("concurrent") & auth_filter)
async def cmd_concurrent(_, msg: Message):
    global CONCURRENT_JOBS
    if len(msg.command) < 2:
        await msg.reply(f"⚡ **Concurrent jobs: `{CONCURRENT_JOBS}`**")
        return
    try:
        val = int(msg.command[1].strip())
        if not 1 <= val <= 5: raise ValueError
    except ValueError:
        await msg.reply("❌ Value must be between 1 and 5.")
        return
    CONCURRENT_JOBS = val
    await msg.reply(f"⚡ **Concurrent jobs set to: `{CONCURRENT_JOBS}`**")
    await save_config_to_mongo()


@app.on_message(filters.command("dl") & auth_filter)
async def cmd_dl(_, msg: Message):
    if len(msg.command) < 2:
        await msg.reply("❌ Usage: `/dl Remote:path`")
        return
    user_id = msg.from_user.id
    if user_id in active_sessions:
        await msg.reply("⚠️ Job already running.")
        return
    remote_path = msg.command[1].strip()
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
    await msg.reply(f"📂 **Choose upload mode for:**\n`{remote_path}`", reply_markup=mode_keyboard(remote_path, del_on, size_on))


@app.on_message(filters.command("retry") & auth_filter)
async def cmd_retry(_, msg: Message):
    user_id = msg.from_user.id
    if user_id in active_sessions:
        await msg.reply("⚠️ Job already running.")
        return
    with state_lock:
        files = list(last_failed)
        remote_path = last_job_remote
        mode = last_job_mode
        do_delete = delete_after_upload
    if not files or not remote_path:
        await msg.reply("ℹ️ Nothing to retry.")
        return
    status_msg = await msg.reply(f"🔁 Retrying **{len(files)}** failed file(s)…")
    results = await _run_job(status_msg, files, remote_path, mode, user_id, do_delete)
    await _send_summary(status_msg, results)


@app.on_message(filters.command("setrclone") & auth_filter)
async def cmd_setrclone(_, msg: Message):
    user_id = msg.from_user.id
    awaiting_rclone_conf.add(user_id)
    await msg.reply("📤 **Send your `rclone.conf` file now** as a Telegram document.")


@app.on_message(filters.document & auth_filter)
async def on_rclone_conf_document(_, msg: Message):
    user_id = msg.from_user.id
    if user_id not in awaiting_rclone_conf: return
    awaiting_rclone_conf.discard(user_id)
    tmp_path = os.path.join(DOWNLOAD_DIR, f"rclone_upload_{user_id}.conf")
    await msg.download(file_name=tmp_path)
    try:
        text = Path(tmp_path).read_text(encoding="utf-8")
        RCLONE_CONF_PATH.parent.mkdir(parents=True, exist_ok=True)
        RCLONE_CONF_PATH.write_text(text, encoding="utf-8")
        os.environ["RCLONE_CONFIG"] = str(RCLONE_CONF_PATH)
        await msg.reply("✅ **rclone.conf updated successfully.**")
    except Exception as e:
        await msg.reply(f"❌ Failed to handle config: {e}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)


@app.on_callback_query(auth_filter)
async def cb_mode(_, cq: CallbackQuery):
    global delete_after_upload, max_size_enabled
    if cq.data.startswith("deltoggle:"):
        _, action, remote_path = cq.data.split(":", 2)
        with state_lock: delete_after_upload = (action == "delon")
        await cq.message.edit(f"📂 **Choose upload mode for:**\n`{remote_path}`", reply_markup=mode_keyboard(remote_path, delete_after_upload, max_size_enabled))
        await save_config_to_mongo()
        return
    if cq.data.startswith("sizetoggle:"):
        _, action, remote_path = cq.data.split(":", 2)
        with state_lock: max_size_enabled = (action == "sizeon")
        await cq.message.edit(f"📂 **Choose upload mode for:**\n`{remote_path}`", reply_markup=mode_keyboard(remote_path, delete_after_upload, max_size_enabled))
        await save_config_to_mongo()
        return
    if not cq.data.startswith("mode:"): return
    _, chosen, remote_path = cq.data.split(":", 2)
    mode = "video" if chosen == "video" else "document"
    user_id = cq.from_user.id
    with state_lock: do_delete = delete_after_upload

    await cq.message.edit(f"📂 `{remote_path}`\n🔍 Listing files…")
    try:
        files = await asyncio.get_running_loop().run_in_executor(None, rclone_list, remote_path)
    except Exception as e:
        await cq.message.edit(f"❌ Failed to list: {e}")
        return

    if not files:
        await cq.message.edit("❌ No files found.")
        return

    size_map = await asyncio.get_running_loop().run_in_executor(None, rclone_size_map, remote_path) if max_size_enabled else None
    results = await _run_job(cq.message, files, remote_path, mode, user_id, do_delete, size_map=size_map)
    await _send_summary(cq.message, results)

# ── Entry point ───────────────────────────────────────────────
async def main():
    await load_config_from_mongo()
    asyncio.create_task(task_check_loop())
    await app.start()
    log.info("Pyrogram client framework active.")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    if psutil is not None:
        try: psutil.cpu_percent(interval=None)
        except Exception: pass
    threading.Thread(target=_run_health, daemon=True).start()
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
