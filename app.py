import base64
import json
import logging
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from live_restream import RESTREAMER
from yt_studio_uploader import open_yt_studio_login, upload_via_yt_studio

# ---------------------------------------------------------------------------
# Paths & Environment (Windows & Ubuntu EC2 ~/clipper-app)
# ---------------------------------------------------------------------------
SRC_DIR = Path(__file__).resolve().parent
APP_ROOT = SRC_DIR.parent if (SRC_DIR.parent / "temp_buffers").exists() else SRC_DIR

TEMP_DIR = Path(os.environ.get("CLIPPER_TEMP_DIR", APP_ROOT / "temp_buffers"))
CLIPS_DIR = Path(os.environ.get("CLIPPER_CLIPS_DIR", APP_ROOT / "finished_clips"))
UPLOADED_DIR = Path(os.environ.get("CLIPPER_UPLOADED_DIR", APP_ROOT / "uploaded_shorts"))
LOGS_DIR = Path(os.environ.get("CLIPPER_LOGS_DIR", APP_ROOT / "logs"))
DATA_DIR = Path(os.environ.get("CLIPPER_DATA_DIR", APP_ROOT / "data"))
STATIC_DIR = SRC_DIR / "static"
DB_PATH = DATA_DIR / "studio.db"

for d in (TEMP_DIR, CLIPS_DIR, UPLOADED_DIR, LOGS_DIR, DATA_DIR, STATIC_DIR):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "clipper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("clipper")

app = FastAPI(title="Clipping Studio — Free 24/7 Edition", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:8765", "http://localhost:8765"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/clips-media", StaticFiles(directory=str(CLIPS_DIR)), name="clips-media")
app.mount("/uploaded-media", StaticFiles(directory=str(UPLOADED_DIR)), name="uploaded-media")

# 1-at-a-time FFmpeg render lock so simultaneous hype spikes never freeze CPU
RENDER_LOCK = threading.Lock()
TRANSCRIBE_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# SQLite Persistence
# ---------------------------------------------------------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS youtube_channels (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            handle_or_url TEXT NOT NULL,
            default_privacy TEXT DEFAULT 'private',
            client_secrets_path TEXT DEFAULT '',
            default_hashtags TEXT DEFAULT '#shorts #clips',
            auto_upload INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(clip_id, channel_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS creators (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            platform TEXT NOT NULL,
            url TEXT NOT NULL,
            target_channel_id TEXT DEFAULT '',
            auto_record INTEGER DEFAULT 1,
            vertical_9_16 INTEGER DEFAULT 1,
            burn_captions INTEGER DEFAULT 1,
            min_score INTEGER DEFAULT 70,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clips (
            id TEXT PRIMARY KEY,
            creator_id TEXT NOT NULL,
            channel_id TEXT DEFAULT '',
            source_type TEXT DEFAULT 'live',
            title TEXT NOT NULL,
            description TEXT DEFAULT '',
            reason TEXT,
            hashtags TEXT,
            tags TEXT DEFAULT '[]',
            score INTEGER DEFAULT 75,
            duration_sec INTEGER DEFAULT 40,
            aspect TEXT DEFAULT '9:16',
            file_path TEXT NOT NULL,
            transcript TEXT,
            status TEXT DEFAULT 'pending',
            youtube_video_id TEXT DEFAULT '',
            synced_drive INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS vod_jobs (
            id TEXT PRIMARY KEY,
            creator_id TEXT NOT NULL,
            url TEXT NOT NULL,
            status TEXT NOT NULL,
            progress TEXT,
            clips_created INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    for alter in (
        "ALTER TABLE youtube_channels ADD COLUMN auto_upload INTEGER DEFAULT 0",
        "ALTER TABLE creators ADD COLUMN target_channel_id TEXT DEFAULT ''",
        "ALTER TABLE clips ADD COLUMN channel_id TEXT DEFAULT ''",
        "ALTER TABLE clips ADD COLUMN description TEXT DEFAULT ''",
        "ALTER TABLE clips ADD COLUMN tags TEXT DEFAULT '[]'",
        "ALTER TABLE clips ADD COLUMN youtube_video_id TEXT DEFAULT ''",
        "ALTER TABLE clips ADD COLUMN upload_attempts INTEGER DEFAULT 0",
        "ALTER TABLE clips ADD COLUMN next_retry_at REAL DEFAULT 0",
        "ALTER TABLE clips ADD COLUMN upload_error TEXT DEFAULT ''",
        "ALTER TABLE clips ADD COLUMN archived INTEGER DEFAULT 0",
        "ALTER TABLE clips ADD COLUMN risk_flags TEXT DEFAULT '[]'",
    ):
        try:
            cur.execute(alter)
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clip_uploads (
            id TEXT PRIMARY KEY,
            clip_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            youtube_video_id TEXT DEFAULT '',
            attempts INTEGER DEFAULT 0,
            next_retry_at REAL DEFAULT 0,
            error TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
        """
    )

    for alter in (
        "ALTER TABLE clip_uploads ADD COLUMN uploaded_at REAL DEFAULT 0",
        "CREATE INDEX IF NOT EXISTS idx_clip_uploads_status ON clip_uploads(status, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_clips_creator ON clips(creator_id, created_at)",
    ):
        try:
            cur.execute(alter)
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()


init_db()


def get_setting(key: str, default: str = "") -> str:
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    conn = get_db()
    conn.execute(
        "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Offline Free Speech-to-Text (faster-whisper with 2-4 word chunking)
# ---------------------------------------------------------------------------
_whisper_model = None
_whisper_lock = threading.Lock()


def get_whisper_model():
    global _whisper_model
    if _whisper_model is not None:
        return _whisper_model
    with _whisper_lock:
        if _whisper_model is not None:
            return _whisper_model
        try:
            from faster_whisper import WhisperModel

            model_size = get_setting("whisper_model", "tiny")
            _whisper_model = WhisperModel(model_size, device="cpu", compute_type="int8")
            logger.info("Loaded faster-whisper model: %s", model_size)
            return _whisper_model
        except Exception as exc:
            logger.warning("faster-whisper unavailable: %s", exc)
            return None


def transcribe_segment(video_path: Path) -> List[Dict[str, Any]]:
    with TRANSCRIBE_LOCK:
        return _transcribe_segment_locked(video_path)

def _transcribe_segment_locked(video_path: Path) -> List[Dict[str, Any]]:
    model = get_whisper_model()
    if model is None:
        return []
    try:
        segments, _ = model.transcribe(
            str(video_path), beam_size=1, vad_filter=True, word_timestamps=True
        )
        chunks: List[Dict[str, Any]] = []
        for seg in segments:
            words = getattr(seg, "words", None) or []
            if not words:
                text = seg.text.strip()
                if text:
                    chunks.append({"start": round(seg.start, 2), "end": round(seg.end, 2), "text": text})
                continue
            buf = []
            for w in words:
                wt = w.word.strip()
                if not wt:
                    continue
                buf.append((w.start, w.end, wt))
                if len(buf) >= 3 or wt.endswith((".", "!", "?", ",")):
                    chunks.append(
                        {
                            "start": round(buf[0][0], 2),
                            "end": round(buf[-1][1], 2),
                            "text": " ".join(x[2] for x in buf),
                        }
                    )
                    buf = []
            if buf:
                chunks.append(
                    {
                        "start": round(buf[0][0], 2),
                        "end": round(buf[-1][1], 2),
                        "text": " ".join(x[2] for x in buf),
                    }
                )
        return chunks
    except Exception as exc:
        logger.warning("Transcription failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Runtime State per Creator
# ---------------------------------------------------------------------------
SEG_SECONDS = 5            # nominal segment length (actual length snaps to keyframes)
RING_SEGMENTS = 100        # ~8 minutes of rolling buffer per creator
PRE_SEGS = 2               # segments kept before the peak segment
POST_SEGS = 3              # segments needed after the peak segment
CLIP_BEFORE = 10.0         # seconds kept before the peak moment
CLIP_AFTER = 15.0          # seconds kept after the peak moment
CHAT_LAG_SEC = 3.0         # wait this long after a segment closes so chat reactions can arrive
CHAT_MIN = 8               # absolute minimum chat messages in one segment to count as a chat spike
MAX_PARALLEL_ANALYSIS = 3  # Whisper+Gemini+render jobs allowed at once per app

ANALYSIS_SLOTS = threading.BoundedSemaphore(MAX_PARALLEL_ANALYSIS)
TWITCH_IRC = os.environ.get("CLIPPER_TWITCH_IRC", "irc.chat.twitch.tv:6667")


class CreatorRuntime:
    def __init__(self, creator_id: str):
        self.creator_id = creator_id
        self.gen = 0  # bumped on every (re)start so stale worker/listener threads exit
        self.is_live = False
        self.is_recording = False
        self.stream_title = "Offline"
        self.started_at: Optional[float] = None
        self.buffer_minutes = 0
        self.ai_checks = 0
        self.quiet_skipped = 0
        self.analysis_dropped = 0
        self.loudness_history: List[float] = [0.0] * 40
        self.chat_times: deque = deque(maxlen=20000)  # wall-clock time of every chat message
        self.last_transcript = "Waiting for speech..."
        self.activity_log: List[str] = []
        self.stop_event = threading.Event()

    def log(self, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.activity_log.insert(0, f"[{ts}] {msg}")
        self.activity_log = self.activity_log[:80]
        logger.info("[%s] %s", self.creator_id, msg)


RUNTIMES: Dict[str, CreatorRuntime] = {}


def get_runtime(creator_id: str) -> CreatorRuntime:
    if creator_id not in RUNTIMES:
        RUNTIMES[creator_id] = CreatorRuntime(creator_id)
    return RUNTIMES[creator_id]


def chat_count(rt: CreatorRuntime, t0: float, t1: float) -> int:
    return sum(1 for t in list(rt.chat_times) if t0 <= t <= t1)


def chat_bins(rt: CreatorRuntime, bins: int = 30, width: float = 2.0) -> List[int]:
    """Chat messages per `width` seconds for the last bins*width seconds (oldest first)."""
    now = time.time()
    out = [0] * bins
    for t in list(rt.chat_times):
        age = now - t
        if 0 <= age < bins * width:
            out[bins - 1 - int(age // width)] += 1
    return out


def twitch_chat_listener(channel: str, rt: CreatorRuntime, gen: int) -> None:
    """Anonymous Twitch IRC reader. Parses line by line (PINGs can arrive glued to chat),
    answers every PING, and reconnects with backoff whenever the socket drops."""
    chan = channel.lower().lstrip("#")
    host, _, port = TWITCH_IRC.partition(":")
    port_n = int(port or 6667)
    backoff = 2.0
    while not rt.stop_event.is_set() and rt.is_recording and rt.gen == gen:
        sock = None
        try:
            sock = socket.create_connection((host, port_n), timeout=10)
            sock.sendall(b"PASS SCHMOOPIIE\r\n")
            sock.sendall(f"NICK justinfan{int(time.time()) % 99999}\r\n".encode())
            sock.sendall(f"JOIN #{chan}\r\n".encode())
            sock.settimeout(1.0)
            buf = b""
            last_data = time.time()
            while not rt.stop_event.is_set() and rt.is_recording and rt.gen == gen:
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    if time.time() - last_data > 360:  # Twitch pings every ~5 min even in empty chats
                        raise ConnectionError("no data for 6 minutes")
                    continue
                if not data:
                    raise ConnectionError("server closed the connection")
                last_data = time.time()
                backoff = 2.0
                buf += data
                while b"\n" in buf:
                    raw, buf = buf.split(b"\n", 1)
                    line = raw.decode("utf-8", errors="ignore").rstrip("\r")
                    if line.startswith("PING"):
                        sock.sendall(("PONG" + line[4:] + "\r\n").encode())
                    elif " PRIVMSG " in line:
                        rt.chat_times.append(time.time())
        except Exception as exc:
            if not rt.stop_event.is_set():
                logger.info("[%s] Twitch chat reconnecting in %.0fs (%s)", rt.creator_id, backoff, exc)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
        if rt.stop_event.wait(backoff):
            break
        backoff = min(30.0, backoff * 2)


# ---------------------------------------------------------------------------
# Helpers & Gemini AI SEO Scoring
# ---------------------------------------------------------------------------
def check_binary(cmd: List[str]) -> bool:
    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        return True
    except Exception:
        return False


def check_whisper_installed() -> bool:
    try:
        import faster_whisper  # noqa: F401
        return True
    except ImportError:
        return False


def yt_dlp_base_cmd() -> List[str]:
    cmd = ["yt-dlp"]
    if shutil.which("deno"):
        cmd.extend(["--js-runtimes", "deno"])
    elif shutil.which("node"):
        cmd.extend(["--js-runtimes", "node"])
    return cmd


def measure_db(video_path: Path) -> Optional[float]:
    """Mean audio volume in dB. -vn means ffmpeg never decodes the video track (~10x faster)."""
    try:
        null_dev = "NUL" if os.name == "nt" else "-"
        res = subprocess.run(
            [
                "ffmpeg", "-nostdin", "-hide_banner", "-i", str(video_path),
                "-vn", "-sn", "-dn", "-af", "volumedetect", "-f", "null", null_dev,
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        m = re.search(r"mean_volume:\s*(-?\d+(?:\.\d+)?)\s*dB", res.stderr)
        if m:
            return float(m.group(1))
    except Exception:
        pass
    return None


def db_to_score(db: Optional[float]) -> float:
    """Map dB to the 0-100 scale used by the UI bars and the Gemini prompt."""
    if db is None:
        return 25.0
    return round(max(5.0, min(100.0, (db + 50.0) * (95.0 / 45.0))), 1)


def measure_loudness(video_path: Path) -> float:
    return db_to_score(measure_db(video_path))


def _median(vals) -> float:
    s = sorted(vals)
    n = len(s)
    if n == 0:
        return 0.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


class PeakDetector:
    """Per-creator adaptive peak finder over 5-second segments.

    A segment is a *hit* when its loudness is `delta_db` above this creator's own recent median
    (or chat is far above its own baseline). The loudest hit in a short window becomes the peak;
    once POST_SEGS more segments exist the event is emitted so the clip can be cut around the peak.
    """

    def __init__(self, delta_db: float = 5.0, prefilter_on: bool = True, abs_floor: float = 45.0):
        self.delta_db = delta_db
        self.prefilter_on = prefilter_on
        self.abs_floor = abs_floor
        self.hist_db: deque = deque(maxlen=60)
        self.hist_chat: deque = deque(maxlen=60)
        self.active: Optional[Dict[str, Any]] = None
        self.cooldown_until = -1

    def feed(self, idx: int, db: float, chat_n: int, mapped: float) -> Dict[str, Any]:
        """Returns {'hit': bool, 'gain': float, 'event': Optional[dict]}."""
        silent = db <= -60.0
        warm = len(self.hist_db) >= 12
        gain = 0.0
        if warm:
            base_db = _median(self.hist_db)
            base_chat = max(1.0, _median(self.hist_chat))
            gain = db - base_db
            loud_hit = (not silent) and gain >= self.delta_db
            chat_hit = chat_n >= max(CHAT_MIN, 2.5 * base_chat)
            hit = loud_hit or chat_hit
            signal = gain + 2.0 * min(chat_n / base_chat, 6.0)
        else:  # first minute: no baseline yet, fall back to the absolute floor
            hit = (not silent) and (mapped >= self.abs_floor or chat_n >= CHAT_MIN)
            signal = mapped / 10.0 + chat_n
        if not self.prefilter_on and not silent:
            hit = True
        if not silent:
            self.hist_db.append(db)
        self.hist_chat.append(chat_n)

        event = None
        if self.active is None:
            if hit and idx > self.cooldown_until:
                self.active = {"first_idx": idx, "peak_idx": idx, "signal": signal, "db": db, "chat": chat_n, "gain": gain}
        else:
            a = self.active
            if hit and signal > a["signal"] and (idx - a["first_idx"] < PRE_SEGS):
                a.update({"peak_idx": idx, "signal": signal, "db": db, "chat": chat_n, "gain": gain})
            if idx - a["peak_idx"] >= POST_SEGS:
                event = dict(a, last_idx=idx)
                self.active = None
                self.cooldown_until = idx + 2
        return {"hit": hit, "gain": gain, "event": event}


def get_duration_sec(video_path: Path) -> float:
    try:
        res = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return float(res.stdout.strip())
    except Exception:
        return 0.0


_gemini_models_cache: Dict[str, Any] = {"t": 0.0, "models": []}
_gemini_cooldown_until = 0.0
_gemini_cooldown_lock = threading.Lock()

SPOILER_TERMS = (
    "spoiler", "cutscene", "cut scene", "ending", "credits", "plot", "twist", "leaked", "leak",
    "final mission", "story mission", "big reveal", "dies", "killed off",
)
GTA_RE = re.compile(r"\bgta\s*(?:6|six|vi)\b|grand theft auto\s*(?:6|six|vi)\b|\bleonida\b", re.I)


def spoiler_flags(*texts: str) -> List[str]:
    blob = " ".join(t for t in texts if t).lower()
    return [t for t in SPOILER_TERMS if re.search(r"\b" + re.escape(t) + r"\b", blob)]


def get_gemini_models(api_key: str) -> List[str]:
    models = ["gemini-3.8-flash", "gemini-2.5-flash", "gemini-2.5-flash-lite"]
    now = time.time()
    if _gemini_models_cache["models"] and now - _gemini_models_cache["t"] < 3600:
        return list(_gemini_models_cache["models"])
    try:
        with httpx.Client(timeout=10.0) as client:
            r = client.get(f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}")
            if r.status_code == 200:
                for m in r.json().get("models", []):
                    name = m.get("name", "").replace("models/", "")
                    methods = m.get("supportedGenerationMethods", [])
                    if "generateContent" in methods and "flash" in name.lower() and name not in models:
                        models.append(name)
    except Exception:
        pass
    _gemini_models_cache.update({"t": now, "models": list(models)})
    return models



def extract_midpoint_frame_b64(video_path: Path) -> Optional[str]:
    dur = get_duration_sec(video_path)
    mid = max(1.0, dur * 0.5)
    jpg_path = video_path.with_suffix(".jpg")
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-ss", f"{mid:.2f}", "-i", str(video_path), "-frames:v", "1", "-vf", "scale=640:-2", "-q:v", "5", str(jpg_path)],
            capture_output=True,
            timeout=10,
        )
        if jpg_path.exists() and jpg_path.stat().st_size > 500:
            b64 = base64.b64encode(jpg_path.read_bytes()).decode("ascii")
            jpg_path.unlink(missing_ok=True)
            return b64
    except Exception:
        pass
    jpg_path.unlink(missing_ok=True)
    return None

def score_moment(
    transcript_text: str,
    loudness_peak: float,
    chat_peak: int,
    creator_name: str = "",
    stream_title: str = "",
    video_path: Optional[Path] = None,
) -> Dict[str, Any]:
    global _gemini_cooldown_until
    api_key = get_setting("gemini_api_key", os.environ.get("GEMINI_API_KEY", "")).strip()
    is_gta = bool(GTA_RE.search(f"{transcript_text} {stream_title} {creator_name}"))
    topic_line = (
        "Topic: Grand Theft Auto VI (GTA 6, GTA VI) launch-week content. Put 'GTA 6' and 'GTA VI' in the tags "
        "and hashtags when the clip is about the game.\n"
        if is_gta
        else ""
    )

    if api_key and transcript_text.strip() and time.time() >= _gemini_cooldown_until:
        prompt = (
            "You are a YouTube Shorts editor. Evaluate this stream/video segment and return ONLY valid JSON:\n"
            '{"score": int (0-100), '
            '"title": "Specific, accurate Shorts title under 65 chars describing what happens", '
            '"description": "1-2 sentences summarizing the clip for YouTube search", '
            '"hashtags": "#shorts #clips #tag3 #tag4", '
            '"tags": ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6"], '
            '"spoiler_risk": true or false, '
            '"reason": "1 sentence explaining why viewers will watch till the end"}\n\n'
            "Rules:\n"
            "- The title must NEVER reveal plot twists, endings, character deaths, mission outcomes or leaked/cutscene content.\n"
            "- Set spoiler_risk=true if the segment shows or discusses story content, cutscenes, endings, leaks, "
            "or anything a viewer who has not played would call a spoiler.\n"
            "- Score high only for a clear peak: a funny, surprising, skillful or emotional moment with a hook.\n"
            f"{topic_line}"
            f"Creator: {creator_name}\n"
            f"Stream title: {stream_title}\n"
            f"Loudness (0-100): {loudness_peak}\n"
            f"Chat peak/2s: {chat_peak}\n"
            f"Transcript:\n{transcript_text[:1800]}"
        )
        parts: List[Dict[str, Any]] = [{"text": prompt}]
        if video_path is not None:
            fb64 = extract_midpoint_frame_b64(video_path)
            if fb64:
                parts.append({"inlineData": {"mimeType": "image/jpeg", "data": fb64}})
        payload = {"contents": [{"parts": parts}], "generationConfig": {"temperature": 0.35}}
        limited = 0
        tried = 0
        for model_name in get_gemini_models(api_key):
            tried += 1
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
            try:
                with httpx.Client(timeout=20.0) as client:
                    r = client.post(url, json=payload)
                if r.status_code == 429:
                    limited += 1
                    continue
                if r.status_code == 200:
                    raw = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                    m_json = re.search(r"\{.*\}", raw, re.DOTALL)
                    if m_json:
                        raw = m_json.group(0)
                    parsed = json.loads(raw)
                    tags = parsed.get("tags", ["shorts", "clips", "gaming"])
                    if not isinstance(tags, list):
                        tags = ["shorts", "clips"]
                    tags = [str(t) for t in tags[:10]]
                    hashtags = str(parsed.get("hashtags", "#shorts #clips #viral"))
                    if is_gta:
                        for t in ("GTA 6", "GTA VI"):
                            if t.lower() not in [x.lower() for x in tags]:
                                tags.append(t)
                    title = str(parsed.get("title", "Stream moment"))[:70]
                    desc = str(parsed.get("description", ""))[:300]
                    flags = spoiler_flags(title, desc, transcript_text)
                    if bool(parsed.get("spoiler_risk", False)):
                        flags = list(dict.fromkeys(["gemini_spoiler_risk"] + flags))
                    return {
                        "score": int(parsed.get("score", 65)),
                        "title": title,
                        "description": desc,
                        "reason": f"[{model_name}] " + str(parsed.get("reason", "Gemini flagged high engagement.")),
                        "hashtags": hashtags,
                        "tags": tags,
                        "risk_flags": flags,
                    }
            except Exception as exc:
                logger.warning("Gemini %s error: %s", model_name, exc)
        if tried and limited == tried:
            with _gemini_cooldown_lock:
                _gemini_cooldown_until = time.time() + 120
            logger.warning("Gemini rate-limited on every model; using local scoring for 120s")

    words = transcript_text.lower().split()
    hype_words = {"what", "no", "wait", "bro", "clutch", "insane", "crazy", "win", "lol", "omg", "gol", "goal"}
    hype_hits = sum(1 for w in words if w.strip(".,!?") in hype_words)
    exclamations = transcript_text.count("!") + transcript_text.count("?")
    score = int(min(98, 35 + (loudness_peak * 0.35) + min(20, chat_peak * 3) + hype_hits * 5 + exclamations * 4))
    snippet = " ".join(transcript_text.split()[:6]).upper() or "HIGHLIGHT CLIP"
    tags = ["shorts", "clips", "gaming", creator_name.lower() or "stream"]
    hashtags = "#shorts #clips #gaming"
    if is_gta:
        tags = ["GTA 6", "GTA VI", "Grand Theft Auto VI"] + tags
        hashtags = "#shorts #gta6 #gtavi #clips"
    return {
        "score": score,
        "title": snippet[:55],
        "description": f"{creator_name} highlight clip.",
        "reason": f"Local signal score (loudness {loudness_peak:.0f}, chat {chat_peak}, hype {hype_hits}).",
        "hashtags": hashtags,
        "tags": tags,
        "risk_flags": spoiler_flags(snippet, transcript_text),
    }



# ---------------------------------------------------------------------------
# YouTube Data API v3 uploader (official API only; no browser automation)
# ---------------------------------------------------------------------------
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


class AuthNeeded(RuntimeError):
    """The channel has no usable login. Pause that channel instead of burning retries."""


class UploadPaused(RuntimeError):
    """YouTube refused because a daily upload/quota limit was hit. Pause that channel."""


def parse_channel_ids(raw: str) -> List[str]:
    return [x.strip() for x in (raw or "").split(",") if x.strip()]


def get_youtube_service(channel_row: Dict[str, Any], allow_interactive_auth: bool = False):
    try:
        from google.auth.exceptions import RefreshError
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError(
            "Install YouTube upload libraries: pip install google-api-python-client google-auth-oauthlib"
        ) from exc

    raw_sec = (channel_row.get("client_secrets_path") or "").strip().strip('"')
    secrets_path = Path(raw_sec) if raw_sec else (SRC_DIR / "client_secrets.json")
    if secrets_path.is_dir():
        secrets_path = secrets_path / "client_secrets.json"
    if not secrets_path.is_file():
        raise AuthNeeded(
            f"client_secrets.json not found (checked {secrets_path}). "
            "Either place client_secrets.json in your clipper folder, or click 'Login YT Studio' to sign in without an API file."
        )

    token_path = DATA_DIR / f"yt_token_{channel_row['id']}.json"
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), YOUTUBE_SCOPES)
    if not creds or not creds.valid:
        refreshed = False
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                refreshed = True
            except RefreshError as exc:
                if not allow_interactive_auth:
                    raise AuthNeeded(
                        "Google rejected the saved login (expired or revoked). If your OAuth consent screen is in "
                        "'Testing' mode, tokens expire after about 7 days. Click 'API Auth' again."
                    ) from exc
        if not refreshed and allow_interactive_auth:
            flow = InstalledAppFlow.from_client_secrets_file(str(secrets_path), YOUTUBE_SCOPES)
            creds = flow.run_local_server(port=8766, open_browser=True)
        else:
            raise AuthNeeded("Channel not authorized yet. Click 'API Auth' in My YouTube Channels.")
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return build("youtube", "v3", credentials=creds)


def enqueue_clip_for_channels(clip_id: str, creator_id: str, fallback_channel_id: str = "") -> List[str]:
    """Create clip_uploads rows for an approved clip. Returns the channel ids queued.

    Only pinned channels are used. With no pin, a single auto-upload channel is used; with several
    candidates nothing is queued (guessing would publish to the wrong channel)."""
    conn = get_db()
    c_row = conn.execute("SELECT target_channel_id FROM creators WHERE id = ?", (creator_id,)).fetchone()
    raw_targets = fallback_channel_id or (c_row["target_channel_id"] if c_row else "") or ""
    target_ids = parse_channel_ids(raw_targets)

    if not target_ids:
        rows = conn.execute("SELECT id FROM youtube_channels WHERE auto_upload = 1").fetchall()
        if len(rows) == 1:
            target_ids = [rows[0]["id"]]
        elif len(rows) > 1:
            logger.warning(
                "Clip %s approved but creator %s has no pinned channel and %d auto-upload channels exist; not queued.",
                clip_id, creator_id, len(rows),
            )

    now = datetime.now().isoformat(timespec="seconds")
    queued: List[str] = []
    for ch_id in target_ids:
        exists = conn.execute(
            "SELECT id, status FROM clip_uploads WHERE clip_id = ? AND channel_id = ?", (clip_id, ch_id)
        ).fetchone()
        if exists and exists["status"] in ("cancelled", "expired", "failed"):
            # a human re-approved it: give it a fresh place in the queue
            conn.execute(
                "UPDATE clip_uploads SET status = 'pending', attempts = 0, next_retry_at = 0, error = '', created_at = ? WHERE id = ?",
                (now, exists["id"]),
            )
        elif not exists:
            conn.execute(
                "INSERT INTO clip_uploads(id, clip_id, channel_id, status, created_at) VALUES(?, ?, ?, 'pending', ?)",
                (uuid.uuid4().hex[:10], clip_id, ch_id, now),
            )
        queued.append(ch_id)
    conn.commit()
    conn.close()
    return queued


def _clean_tags(tags: List[str]) -> List[str]:
    out: List[str] = []
    total = 0
    for t in tags:
        t = str(t).strip().lstrip("#")
        if not t or len(t) > 60:
            continue
        if total + len(t) + 1 > 450:  # YouTube caps the combined tag length at 500
            break
        out.append(t)
        total += len(t) + 1
    return out


def _is_limit_error(exc: Exception) -> bool:
    txt = str(exc).lower()
    return any(k in txt for k in ("uploadlimitexceeded", "quotaexceeded", "dailylimitexceeded", "ratelimitexceeded"))


def upload_clip_to_channel(clip_id: str, channel_id: str) -> str:
    from googleapiclient.http import MediaFileUpload

    conn = get_db()
    clip_row = conn.execute("SELECT * FROM clips WHERE id = ?", (clip_id,)).fetchone()
    ch_row = conn.execute("SELECT * FROM youtube_channels WHERE id = ?", (channel_id,)).fetchone()
    up_row = conn.execute(
        "SELECT id, status, youtube_video_id FROM clip_uploads WHERE clip_id = ? AND channel_id = ?", (clip_id, channel_id)
    ).fetchone()
    conn.close()
    if not clip_row or not ch_row:
        raise RuntimeError("Clip or YouTube channel not found")
    if up_row and up_row["status"] == "uploaded" and up_row["youtube_video_id"]:
        return up_row["youtube_video_id"]
    conn_claim = get_db()
    if up_row:
        c_res = conn_claim.execute("UPDATE clip_uploads SET status = 'uploading' WHERE id = ? AND status IN ('pending', 'failed')", (up_row["id"],))
        conn_claim.commit()
        conn_claim.close()
        if c_res.rowcount == 0:
            raise RuntimeError("Upload already in progress or completed for this channel")
    else:
        conn_claim.execute(
            "INSERT OR IGNORE INTO clip_uploads(id, clip_id, channel_id, status, created_at) VALUES(?, ?, ?, 'uploading', ?)",
            (uuid.uuid4().hex[:10], clip_id, channel_id, datetime.now().isoformat(timespec="seconds")),
        )
        conn_claim.commit()
        conn_claim.close()
    clip = dict(clip_row)
    ch = dict(ch_row)

    video_file = CLIPS_DIR / clip["file_path"]
    if not video_file.exists():
        video_file = UPLOADED_DIR / clip["file_path"]
    if not video_file.exists():
        raise RuntimeError(f"Clip file missing: {clip['file_path']}")

    title = f"{clip['title']} #Shorts"[:100]
    desc = f"{clip.get('description', '')}\n\n{clip.get('hashtags', '')} {ch.get('default_hashtags', '')}".strip()[:4900]
    try:
        tags = _clean_tags(json.loads(clip.get("tags") or "[]"))
    except Exception:
        tags = ["shorts", "clips"]

    profile_dir = DATA_DIR / "yt_profiles" / ch["id"]
    token_path = DATA_DIR / f"yt_token_{ch['id']}.json"
    if profile_dir.exists() and not token_path.exists():
        vid_id = upload_via_yt_studio(
            profile_dir=profile_dir,
            video_path=video_file,
            title=title,
            description=desc,
            tags=tags,
            privacy=ch.get("default_privacy", "private"),
        )
        now_ts = time.time()
        conn = get_db()
        conn.execute("UPDATE clips SET youtube_video_id = ?, status = 'uploaded' WHERE id = ?", (vid_id, clip_id))
        conn.execute("UPDATE clip_uploads SET status = 'uploaded', youtube_video_id = ?, uploaded_at = ?, error = '' WHERE clip_id = ? AND channel_id = ?", (vid_id, now_ts, clip_id, channel_id))
        conn.commit()
        conn.close()
        return vid_id
    yt = get_youtube_service(ch, allow_interactive_auth=False)
    body = {
        "snippet": {"title": title, "description": desc, "tags": tags, "categoryId": "20"},
        "status": {
            "privacyStatus": ch.get("default_privacy") or "private",
            "selfDeclaredMadeForKids": False,
        },
    }
    media = MediaFileUpload(str(video_file), mimetype="video/mp4", resumable=True)
    req = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    resp = None
    try:
        while resp is None:
            _, resp = req.next_chunk(num_retries=3)
    except Exception as exc:
        if _is_limit_error(exc):
            raise UploadPaused(str(exc)[:200]) from exc
        raise
    vid_id = resp.get("id", "")
    if not vid_id:
        raise RuntimeError("YouTube returned no video id")

    now_ts = time.time()
    conn = get_db()
    conn.execute("UPDATE clips SET youtube_video_id = ?, status = 'uploaded', upload_error = '' WHERE id = ?", (vid_id, clip_id))
    if up_row:
        conn.execute(
            "UPDATE clip_uploads SET status = 'uploaded', youtube_video_id = ?, uploaded_at = ?, error = '' WHERE id = ?",
            (vid_id, now_ts, up_row["id"]),
        )
    else:
        conn.execute(
            "INSERT INTO clip_uploads(id, clip_id, channel_id, status, youtube_video_id, uploaded_at, created_at) "
            "VALUES(?, ?, ?, 'uploaded', ?, ?, ?)",
            (uuid.uuid4().hex[:10], clip_id, channel_id, vid_id, now_ts, datetime.now().isoformat(timespec="seconds")),
        )
    conn.commit()
    conn.close()
    logger.info("Uploaded clip %s to channel %s as video %s (%s)", clip_id, channel_id, vid_id, ch.get("default_privacy"))
    return vid_id


def upload_clip_to_youtube(clip_id: str, channel_id: str = "") -> str:
    """Manual 'Upload YT' button: uploads right now to one channel, bypassing the queue budget."""
    conn = get_db()
    clip_row = conn.execute("SELECT * FROM clips WHERE id = ?", (clip_id,)).fetchone()
    if not clip_row:
        conn.close()
        raise RuntimeError("Clip not found")
    clip = dict(clip_row)
    target = channel_id.strip() if channel_id else ""
    if not target:
        ids = parse_channel_ids(clip.get("channel_id", ""))
        if not ids:
            c_row = conn.execute("SELECT target_channel_id FROM creators WHERE id = ?", (clip["creator_id"],)).fetchone()
            ids = parse_channel_ids(c_row["target_channel_id"] if c_row else "")
        target = ids[0] if ids else ""
    conn.close()
    if not target:
        raise RuntimeError("No YouTube channel assigned to this clip")
    return upload_clip_to_channel(clip_id, target)


# ---------------------------------------------------------------------------
# FFmpeg Concat & Protected Render Queue
# ---------------------------------------------------------------------------
def concat_chunks(chunk_paths: List[Path], out_file: Path) -> bool:
    if len(chunk_paths) == 1:
        out_file.write_bytes(chunk_paths[0].read_bytes())
        return True
    list_file = out_file.with_suffix(".txt")
    list_file.write_text("\n".join(f"file '{p.resolve()}'" for p in chunk_paths), encoding="utf-8")
    res = subprocess.run(
        ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(list_file), "-c", "copy", str(out_file)],
        capture_output=True,
        timeout=60,
    )
    list_file.unlink(missing_ok=True)
    return res.returncode == 0 and out_file.exists()


def write_srt(segments: List[Dict[str, Any]], srt_path: Path) -> None:
    def fmt(sec: float) -> str:
        ms = int((sec - int(sec)) * 1000)
        s = int(sec) % 60
        m = (int(sec) // 60) % 60
        h = int(sec) // 3600
        return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

    lines = []
    for i, seg in enumerate(segments, 1):
        lines.extend([str(i), f"{fmt(seg['start'])} --> {fmt(seg['end'])}", seg["text"], ""])
    srt_path.write_text("\n".join(lines), encoding="utf-8")


def shift_segments(segs: List[Dict[str, Any]], start: float, dur: float) -> List[Dict[str, Any]]:
    """Re-time transcript segments to a clip that starts `start` seconds into the source."""
    out = []
    for s in segs:
        if s["end"] <= start or s["start"] >= start + dur:
            continue
        out.append({"start": max(0.0, s["start"] - start), "end": min(dur, s["end"] - start), "text": s["text"]})
    return out


def render_clip(
    source_path: Path,
    out_path: Path,
    vertical_9_16: bool = True,
    burn_captions: bool = True,
    segments: Optional[List[Dict[str, Any]]] = None,
    start_sec: Optional[float] = None,
    duration_sec: Optional[float] = None,
) -> bool:
    """`segments` must already be relative to the start of the rendered clip."""
    with RENDER_LOCK:
        cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-y"]
        if start_sec is not None:
            cmd.extend(["-ss", f"{max(0.0, start_sec):.2f}"])
        cmd.extend(["-i", str(source_path.resolve())])
        if duration_sec is not None:
            cmd.extend(["-t", f"{duration_sec:.2f}"])

        srt_path = out_path.with_suffix(".srt")
        sub_filter = ""
        if burn_captions and segments:
            write_srt(segments, srt_path)
            font_name = "Arial" if os.name == "nt" else "DejaVu Sans"
            sub_filter = (
                f"subtitles='{srt_path.name}':force_style='FontName={font_name},FontSize=20,Bold=1,"
                "PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=2,Alignment=2,MarginV=95'"
            )

        if vertical_9_16:
            # The blurred backdrop is built at quarter resolution (270x480) and scaled up: a blur hides
            # the lost detail anyway and this is ~2x cheaper than blurring a 1080x1920 frame.
            fc = (
                "split[bg][fg];"
                "[bg]scale=270:480:force_original_aspect_ratio=increase,crop=270:480,boxblur=5:2,"
                "scale=1080:1920:flags=bilinear[bg_blur];"
                "[fg]scale=-2:960,crop=1080:960[fg_scaled];"
                "[bg_blur][fg_scaled]overlay=(W-w)/2:(H-h)/2"
            )
            if sub_filter:
                fc += f",{sub_filter}"
            cmd.extend(["-filter_complex", fc])
        elif sub_filter:
            cmd.extend(["-vf", sub_filter])

        cmd.extend(
            [
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", str(out_path.resolve()),
            ]
        )
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=240, cwd=str(CLIPS_DIR))
        if srt_path.exists():
            srt_path.unlink(missing_ok=True)
        if res.returncode != 0:
            logger.warning("ffmpeg render failed: %s", res.stderr[-400:])
        return res.returncode == 0 and out_path.exists()


def maybe_sync_rclone(file_path: Path, clip_id: str) -> None:
    remote = get_setting("rclone_remote", "").strip()
    if not remote or not check_binary(["rclone", "version"]):
        return
    try:
        res = subprocess.run(["rclone", "copy", str(file_path), remote], capture_output=True, timeout=120)
        if res.returncode == 0:
            conn = get_db()
            conn.execute("UPDATE clips SET synced_drive = 1 WHERE id = ?", (clip_id,))
            conn.commit()
            conn.close()
    except Exception:
        pass


def save_clip_record(
    creator_id: str,
    channel_id: str,
    source_type: str,
    title: str,
    description: str,
    reason: str,
    hashtags: str,
    tags: List[str],
    score: int,
    duration_sec: int,
    aspect: str,
    file_path: Path,
    transcript: str,
    risk_flags: Optional[List[str]] = None,
) -> Dict[str, Any]:
    clip_id = uuid.uuid4().hex[:10]
    created_at = datetime.now().isoformat(timespec="seconds")
    flags = list(dict.fromkeys((risk_flags or []) + spoiler_flags(title, description, transcript)))
    auto_thresh = int(get_setting("auto_approve_score", "80") or "80")
    auto_on = get_setting("auto_approve_enabled", "0") == "1"
    # Review gate: auto-approve is OFF by default, and a clip with any spoiler/risk flag is never auto-approved.
    init_status = "approved" if (auto_on and score >= auto_thresh and not flags) else "pending"

    conn = get_db()
    conn.execute(
        """
        INSERT INTO clips(id, creator_id, channel_id, source_type, title, description, reason, hashtags, tags, score, duration_sec, aspect, file_path, transcript, status, created_at, risk_flags)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (clip_id, creator_id, channel_id, source_type, title, description, reason, hashtags, json.dumps(tags), score, duration_sec, aspect, file_path.name, transcript, init_status, created_at, json.dumps(flags)),
    )
    conn.commit()
    conn.close()

    threading.Thread(target=maybe_sync_rclone, args=(file_path, clip_id), daemon=True).start()
    if init_status == "approved":
        enqueue_clip_for_channels(clip_id, creator_id, channel_id)

    return {"id": clip_id, "title": title, "score": score, "file_name": file_path.name, "status": init_status, "risk_flags": flags}


# ---------------------------------------------------------------------------
# Zero-Gap 2-Thread Live Recorder + Loudness/Chat Pre-Filter + Render Queue
# ---------------------------------------------------------------------------
def resolve_live_stream_url(url: str) -> Optional[str]:
    try:
        sl = subprocess.run(["streamlink", "--stream-url", url, "720p,best"], capture_output=True, text=True, timeout=20)
        if sl.returncode == 0 and sl.stdout.strip().startswith("http"):
            return sl.stdout.strip().splitlines()[0]
    except Exception:
        pass
    try:
        yd = subprocess.run(yt_dlp_base_cmd() + ["-g", "-f", "best[height<=720]/best", url], capture_output=True, text=True, timeout=20)
        if yd.returncode == 0 and yd.stdout.strip().startswith("http"):
            return yd.stdout.strip().splitlines()[0]
    except Exception:
        pass
    return None


def get_stream_title(url: str, fallback: str) -> str:
    try:
        res = subprocess.run(yt_dlp_base_cmd() + ["--get-title", "--no-playlist", url], capture_output=True, text=True, timeout=15)
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip().splitlines()[0]
    except Exception:
        pass
    return fallback


def wipe_buffers(c_buf: Path, q_dir: Path) -> None:
    """ffmpeg restarts numbering at seg_00000, so stale files from a previous run would be mistaken
    for new ones. Start every recording with an empty buffer."""
    for folder, pattern in ((c_buf, "seg_*.mp4"), (c_buf, "manual_src_*.mp4")):
        for p in folder.glob(pattern):
            try:
                p.unlink()
            except OSError:
                pass


def analyze_and_render_task(
    creator: Dict[str, Any],
    rt: CreatorRuntime,
    chunks: List[Path],
    peak_pos: int,
    pre_loud: float,
    pre_chat: int,
    stream_title: str = "",
) -> None:
    """Runs in a background thread so continuous recording never pauses.
    `chunks` are private copies of the segments around the peak; `peak_pos` is the peak's position."""
    merged_file = chunks[0].parent / f"q_merged_{uuid.uuid4().hex[:6]}.mp4"
    try:
        if not concat_chunks(chunks, merged_file):
            rt.log("Could not join segments; skipping this moment")
            return
        durs = [get_duration_sec(p) or float(SEG_SECONDS) for p in chunks]
        total = sum(durs)
        center = sum(durs[:peak_pos]) + durs[peak_pos] / 2.0
        start = max(0.0, center - CLIP_BEFORE)
        dur = min(CLIP_BEFORE + CLIP_AFTER, max(5.0, total - start))

        segs_all = transcribe_segment(merged_file)
        win_segs = shift_segments(segs_all, start, dur)
        text = " ".join(s["text"] for s in win_segs)
        if text:
            rt.last_transcript = text

        rt.ai_checks += 1
        result = score_moment(text, pre_loud, pre_chat, creator["name"], stream_title, video_path=cut_file)
        rt.log(f"AI check #{rt.ai_checks}: score {result['score']} - {result['title']}")

        if result["score"] >= int(creator["min_score"]):
            out_path = CLIPS_DIR / f"{creator['id']}_{uuid.uuid4().hex[:8]}.mp4"
            ok = render_clip(
                merged_file,
                out_path,
                vertical_9_16=bool(creator["vertical_9_16"]),
                burn_captions=bool(creator["burn_captions"]),
                segments=win_segs,
                start_sec=start,
                duration_sec=dur,
            )
            if ok:
                rec = save_clip_record(
                    creator_id=creator["id"],
                    channel_id=creator.get("target_channel_id", ""),
                    source_type="live",
                    title=result["title"],
                    description=result.get("description", ""),
                    reason=result["reason"],
                    hashtags=result["hashtags"],
                    tags=result.get("tags", []),
                    score=result["score"],
                    duration_sec=int(round(dur)),
                    aspect="9:16" if creator["vertical_9_16"] else "16:9",
                    file_path=out_path,
                    transcript=text,
                    risk_flags=result.get("risk_flags", []),
                )
                flag_note = f" [review: {', '.join(rec['risk_flags'])}]" if rec["risk_flags"] else ""
                rt.log(f"Clip saved ({int(round(dur))}s, {rec['status']}): {result['title']}{flag_note}")
    except Exception as exc:
        rt.log(f"Analysis failed: {exc}")
        logger.exception("analyze_and_render_task failed")
    finally:
        ANALYSIS_SLOTS.release()
        merged_file.unlink(missing_ok=True)
        for p in chunks:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass


def dispatch_event(
    creator: Dict[str, Any],
    rt: CreatorRuntime,
    q_dir: Path,
    seg_paths: Dict[int, Path],
    ev: Dict[str, Any],
) -> None:
    peak = ev["peak_idx"]
    idxs = [i for i in range(peak - PRE_SEGS, min(peak + POST_SEGS, ev["last_idx"]) + 1) if i in seg_paths]
    if peak not in idxs:
        return
    if not ANALYSIS_SLOTS.acquire(blocking=False):
        rt.analysis_dropped += 1
        rt.log("Analysis backlog full; dropped one moment (raise the threshold or add CPU)")
        return
    try:
        copies: List[Path] = []
        for i in idxs:
            dst = q_dir / f"q_{uuid.uuid4().hex[:6]}_{i:05d}.mp4"
            shutil.copy2(seg_paths[i], dst)
            copies.append(dst)
        peak_pos = idxs.index(peak)
    except Exception as exc:
        ANALYSIS_SLOTS.release()
        rt.log(f"Could not copy segments: {exc}")
        return
    rt.log(f"Peak: +{ev['gain']:.1f} dB over baseline, chat {ev['chat']} in 5s - analyzing")
    pre_loud = db_to_score(ev["db"])
    pre_chat = int(round(ev["chat"] * 2.0 / SEG_SECONDS))  # per-2s units, as the prompt expects
    threading.Thread(
        target=analyze_and_render_task,
        args=(creator, rt, copies, peak_pos, pre_loud, pre_chat, rt.stream_title),
        daemon=True,
    ).start()


def creator_worker(creator_id: str) -> None:
    rt = get_runtime(creator_id)
    rt.gen += 1
    gen = rt.gen
    rt.is_recording = True
    rt.started_at = time.time()
    rt.stop_event.clear()

    c_buf = TEMP_DIR / creator_id
    q_dir = TEMP_DIR / f"{creator_id}_queue"
    c_buf.mkdir(parents=True, exist_ok=True)
    q_dir.mkdir(parents=True, exist_ok=True)

    conn = get_db()
    row = conn.execute("SELECT * FROM creators WHERE id = ?", (creator_id,)).fetchone()
    conn.close()
    if not row:
        rt.is_recording = False
        return
    creator = dict(row)

    def alive() -> bool:
        return (not rt.stop_event.is_set()) and rt.gen == gen

    rt.log(f"Monitor started for {creator['name']}")
    if "twitch.tv/" in creator["url"].lower():
        chan = creator["url"].rstrip("/").split("/")[-1]
        threading.Thread(target=twitch_chat_listener, args=(chan, rt, gen), daemon=True).start()

    while alive():
        stream_url = resolve_live_stream_url(creator["url"])
        if not stream_url:
            rt.is_live = False
            rt.stream_title = "Offline"
            rt.log("Creator offline. Checking again in 30s...")
            if rt.stop_event.wait(30):
                break
            continue

        rt.is_live = True
        rt.stream_title = get_stream_title(creator["url"], creator["name"])
        rt.log(f"Stream LIVE: {rt.stream_title} - starting segment recorder")

        wipe_buffers(c_buf, q_dir)
        seg_pattern = str(c_buf / "seg_%05d.mp4")
        proc = subprocess.Popen(
            [
                "ffmpeg", "-nostdin", "-hide_banner", "-y", "-i", stream_url,
                "-c", "copy", "-f", "segment", "-segment_time", str(SEG_SECONDS),
                "-reset_timestamps", "1", seg_pattern,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        det = PeakDetector()
        seen: set = set()
        seg_paths: Dict[int, Path] = {}

        while alive() and proc.poll() is None:
            time.sleep(2)
            det.delta_db = float(get_setting("trigger_db_delta", "5") or "5")
            det.prefilter_on = get_setting("prefilter_enabled", "1") == "1"
            det.abs_floor = float(get_setting("prefilter_loudness", "45") or "45")

            all_segs = sorted(c_buf.glob("seg_*.mp4"))
            completed = all_segs[:-1] if len(all_segs) > 1 else []  # the newest file is still being written
            rt.buffer_minutes = max(1, (len(all_segs) * SEG_SECONDS) // 60)

            for p in completed:
                if p.name in seen:
                    continue
                try:
                    mtime = p.stat().st_mtime
                except OSError:
                    continue
                if time.time() < mtime + CHAT_LAG_SEC:
                    break  # let chat reactions arrive before judging this segment
                seen.add(p.name)
                try:
                    idx = int(p.stem.split("_")[1])
                except (IndexError, ValueError):
                    continue
                seg_paths[idx] = p

                db = measure_db(p)
                mapped = db_to_score(db)
                rt.loudness_history = rt.loudness_history[1:] + [mapped]
                chat_n = chat_count(rt, mtime - 4.0, mtime + CHAT_LAG_SEC)

                res = det.feed(idx, db if db is not None else -91.0, chat_n, mapped)
                if not res["hit"]:
                    rt.quiet_skipped += 1
                    if rt.quiet_skipped % 24 == 0:
                        rt.log(f"{rt.quiet_skipped} quiet segments skipped so far (loudness {mapped:.0f}, chat {chat_n})")
                if res["event"]:
                    dispatch_event(creator, rt, q_dir, seg_paths, res["event"])

            # Ring buffer: keep the newest RING_SEGMENTS files; never let a Windows file lock kill the worker.
            while len(all_segs) > RING_SEGMENTS:
                old = all_segs.pop(0)
                try:
                    old.unlink(missing_ok=True)
                except OSError:
                    pass
            if seg_paths:
                hi = max(seg_paths)
                for i in [k for k in seg_paths if k < hi - RING_SEGMENTS]:
                    seg_paths.pop(i, None)

        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        if alive():
            rt.log("Recorder ended (stream offline or dropped); re-checking in 10s")
            if rt.stop_event.wait(10):
                break

    if rt.gen == gen:
        rt.is_recording = False
        rt.is_live = False
    rt.log("Monitoring stopped.")


# ---------------------------------------------------------------------------
# Recorded Video / YouTube VOD Worker
# ---------------------------------------------------------------------------
def vod_worker(
    job_id: str,
    creator_id: str,
    url: str,
    max_clips: int,
    clip_duration: int,
    vertical_9_16: bool,
    burn_captions: bool,
) -> None:
    def update_job(status: str, progress: str, clips_created: int = 0):
        conn = get_db()
        conn.execute(
            "UPDATE vod_jobs SET status = ?, progress = ?, clips_created = ? WHERE id = ?",
            (status, progress, clips_created, job_id),
        )
        conn.commit()
        conn.close()

    conn = get_db()
    c_row = conn.execute("SELECT name, target_channel_id FROM creators WHERE id = ?", (creator_id,)).fetchone()
    conn.close()
    target_ch = c_row["target_channel_id"] if c_row else ""
    creator_name = c_row["name"] if c_row else ""

    tmp_vod = TEMP_DIR / f"vod_{job_id}.mp4"
    try:
        update_job("downloading", "Downloading video via yt-dlp...")
        res = subprocess.run(
            yt_dlp_base_cmd() + ["-f", "best[height<=720][ext=mp4]/best[height<=720]", "-o", str(tmp_vod), url],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if res.returncode != 0 or not tmp_vod.exists():
            res = subprocess.run(
                ["yt-dlp", "--extractor-args", "youtube:player_client=android", "-f", "best[height<=720]/best", "-o", str(tmp_vod), url],
                capture_output=True,
                text=True,
                timeout=600,
            )
        if res.returncode != 0 or not tmp_vod.exists():
            update_job("failed", f"Download failed: {res.stderr[:200]}")
            return

        total_dur = get_duration_sec(tmp_vod)
        if total_dur <= 0:
            update_job("failed", "Could not read video duration.")
            return

        update_job("analyzing", f"Transcribing {int(total_dur)}s video offline...")
        all_segs = transcribe_segment(tmp_vod)

        candidates = []
        step = max(clip_duration, 30)
        t = 0.0
        while t + 10 < total_dur:
            win_end = min(total_dur, t + clip_duration)
            win_segs = [
                {"start": s["start"] - t, "end": s["end"] - t, "text": s["text"]}
                for s in all_segs
                if t <= s["start"] < win_end
            ]
            win_text = " ".join(s["text"] for s in win_segs)
            if win_text.strip() or total_dur <= clip_duration * 2:
                ai_res = score_moment(win_text, 55.0, 0, creator_name)
                candidates.append({"start": t, "segs": win_segs, "text": win_text, **ai_res})
            t += step

        candidates.sort(key=lambda x: x["score"], reverse=True)
        selected = candidates[:max_clips]
        update_job("rendering", f"Rendering top {len(selected)} clips...")

        created = 0
        for idx, cand in enumerate(selected, 1):
            out_path = CLIPS_DIR / f"{creator_id}_vod_{job_id}_{idx}.mp4"
            ok = render_clip(
                tmp_vod,
                out_path,
                vertical_9_16=vertical_9_16,
                burn_captions=burn_captions,
                segments=cand["segs"],
                start_sec=cand["start"],
                duration_sec=clip_duration,
            )
            if ok:
                created += 1
                save_clip_record(
                    creator_id=creator_id,
                    channel_id=target_ch,
                    source_type="vod",
                    title=cand["title"],
                    description=cand.get("description", ""),
                    reason=cand["reason"],
                    hashtags=cand["hashtags"],
                    tags=cand.get("tags", []),
                    score=cand["score"],
                    duration_sec=clip_duration,
                    aspect="9:16" if vertical_9_16 else "16:9",
                    file_path=out_path,
                    transcript=cand["text"][:500],
                    risk_flags=cand.get("risk_flags", []),
                )
        update_job("completed", f"Done! Created {created} clips.", created)
    except Exception as exc:
        update_job("failed", str(exc)[:200])
    finally:
        tmp_vod.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Auto-resume 24/7 creators on startup
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Ranked, expiring upload queue (best + freshest first, with daily budgets)
# ---------------------------------------------------------------------------
MAX_UPLOAD_ATTEMPTS = 5
BACKOFF_SECS = [60, 180, 600, 1800, 3600]
FRESHNESS_PENALTY_PER_MIN = 0.25  # a queued clip loses 15 score points per hour of waiting
PAUSED_UNTIL: Dict[str, float] = {}  # channel_id -> unix time until which uploads are paused


def queue_config() -> Dict[str, int]:
    def i(key: str, default: str) -> int:
        try:
            return int(get_setting(key, default) or default)
        except ValueError:
            return int(default)

    return {
        "expiry_min": i("queue_expiry_min", "180"),          # unqueue clips that waited longer than this
        "channel_daily_cap": i("daily_upload_cap", "30"),    # uploads per channel per rolling 24h
        "project_daily_cap": i("project_daily_cap", "90"),   # uploads across all channels per rolling 24h (API limit is 100)
        "min_gap_sec": i("upload_min_gap_sec", "300"),       # spacing between uploads on one channel
    }


def expire_stale_uploads(conn: sqlite3.Connection, now_ts: float, expiry_min: int) -> int:
    cutoff = datetime.fromtimestamp(now_ts - expiry_min * 60).isoformat(timespec="seconds")
    cur = conn.execute(
        "UPDATE clip_uploads SET status = 'expired', error = 'waited too long in queue' "
        "WHERE status = 'pending' AND created_at < ?",
        (cutoff,),
    )
    return cur.rowcount or 0


def pick_next_upload(conn: sqlite3.Connection, now_ts: float, cfg: Dict[str, int]) -> Optional[sqlite3.Row]:
    """Choose the single best pending upload that fits today's budget and pacing, or None."""
    day_ago = now_ts - 86400
    per_channel = {
        r["channel_id"]: (r["n"], r["last"] or 0.0)
        for r in conn.execute(
            "SELECT channel_id, COUNT(*) AS n, MAX(uploaded_at) AS last FROM clip_uploads "
            "WHERE status = 'uploaded' AND uploaded_at > ? GROUP BY channel_id",
            (day_ago,),
        ).fetchall()
    }
    project_total = sum(n for n, _ in per_channel.values())
    if project_total >= cfg["project_daily_cap"]:
        return None

    rows = conn.execute(
        """
        SELECT u.id AS uid, u.clip_id, u.channel_id, u.attempts, u.created_at AS queued_at, c.score
        FROM clip_uploads u
        JOIN clips c ON u.clip_id = c.id
        JOIN youtube_channels yc ON u.channel_id = yc.id
        WHERE c.status IN ('approved', 'uploaded')
          AND u.status = 'pending'
          AND yc.auto_upload = 1
          AND COALESCE(u.attempts, 0) < ?
          AND COALESCE(u.next_retry_at, 0) <= ?
        """,
        (MAX_UPLOAD_ATTEMPTS, now_ts),
    ).fetchall()

    best, best_rank = None, None
    for r in rows:
        ch = r["channel_id"]
        if PAUSED_UNTIL.get(ch, 0) > now_ts:
            continue
        n, last = per_channel.get(ch, (0, 0.0))
        if n >= cfg["channel_daily_cap"] or now_ts - last < cfg["min_gap_sec"]:
            continue
        try:
            age_min = max(0.0, (now_ts - datetime.fromisoformat(r["queued_at"]).timestamp()) / 60.0)
        except ValueError:
            age_min = 0.0
        rank = float(r["score"] or 0) - FRESHNESS_PENALTY_PER_MIN * age_min
        if best_rank is None or rank > best_rank:
            best, best_rank = r, rank
    return best


def upload_queue_worker() -> None:
    """24/7 upload worker: one upload per pass, best-ranked first, within daily budgets."""
    while True:
        try:
            now_ts = time.time()
            cfg = queue_config()
            conn = get_db()
            expired = expire_stale_uploads(conn, now_ts, cfg["expiry_min"])
            conn.commit()
            r = pick_next_upload(conn, now_ts, cfg)
            conn.close()
            if expired:
                logger.info("Expired %d stale queued upload(s)", expired)

            if r is not None:
                try:
                    upload_clip_to_channel(r["clip_id"], r["channel_id"])
                except (AuthNeeded, UploadPaused) as exc:
                    pause = 600 if isinstance(exc, AuthNeeded) else 3600
                    PAUSED_UNTIL[r["channel_id"]] = time.time() + pause
                    conn = get_db()
                    conn.execute("UPDATE clip_uploads SET error = ? WHERE id = ?", (str(exc)[:220], r["uid"]))
                    conn.execute("UPDATE clips SET upload_error = ? WHERE id = ?", (str(exc)[:220], r["clip_id"]))
                    conn.commit()
                    conn.close()
                    logger.warning("Uploads to channel %s paused %ds: %s", r["channel_id"], pause, exc)
                except Exception as exc:
                    attempts = int(r["attempts"] or 0) + 1
                    delay = BACKOFF_SECS[min(attempts - 1, len(BACKOFF_SECS) - 1)]
                    new_status = "failed" if attempts >= MAX_UPLOAD_ATTEMPTS else "pending"
                    conn = get_db()
                    conn.execute(
                        "UPDATE clip_uploads SET attempts = ?, next_retry_at = ?, error = ?, status = ? WHERE id = ?",
                        (attempts, time.time() + delay, str(exc)[:220], new_status, r["uid"]),
                    )
                    conn.execute(
                        "UPDATE clips SET upload_attempts = ?, upload_error = ? WHERE id = ?",
                        (attempts, str(exc)[:220], r["clip_id"]),
                    )
                    conn.commit()
                    conn.close()
                    logger.warning("Upload attempt %d for clip %s -> ch %s failed: %s", attempts, r["clip_id"], r["channel_id"], exc)
        except Exception as exc:
            logger.warning("Upload queue loop warning: %s", exc)
        time.sleep(15)


def archive_worker() -> None:
    """Moves MP4s of fully uploaded clips from finished_clips to uploaded_shorts.
    The API returns a video id only on success, so no re-verification is needed."""
    while True:
        try:
            conn = get_db()
            rows = conn.execute(
                """
                SELECT c.id, c.file_path FROM clips c
                WHERE c.status = 'uploaded' AND COALESCE(c.archived, 0) = 0
                  AND NOT EXISTS (SELECT 1 FROM clip_uploads u WHERE u.clip_id = c.id AND u.status = 'pending')
                LIMIT 20
                """
            ).fetchall()
            conn.close()
            for r in rows:
                src, dst = CLIPS_DIR / r["file_path"], UPLOADED_DIR / r["file_path"]
                try:
                    if src.exists():
                        shutil.move(str(src), str(dst))
                except OSError as exc:  # e.g. rclone still copying it on Windows; retry next pass
                    logger.debug("Archive of %s postponed: %s", r["file_path"], exc)
                    continue
                conn = get_db()
                conn.execute("UPDATE clips SET archived = 1 WHERE id = ?", (r["id"],))
                conn.commit()
                conn.close()
        except Exception as exc:
            logger.debug("Archive worker warning: %s", exc)
        time.sleep(30)


def backfill_clip_uploads() -> None:
    """Approved clips (e.g. approved under v1) that never got queue rows: queue the recent ones."""
    cfg = queue_config()
    cutoff = datetime.fromtimestamp(time.time() - cfg["expiry_min"] * 60).isoformat(timespec="seconds")
    conn = get_db()
    rows = conn.execute(
        """
        SELECT id, creator_id, channel_id FROM clips
        WHERE status = 'approved' AND COALESCE(youtube_video_id, '') = '' AND created_at >= ?
          AND NOT EXISTS (SELECT 1 FROM clip_uploads u WHERE u.clip_id = clips.id)
        """,
        (cutoff,),
    ).fetchall()
    conn.close()
    for r in rows:
        enqueue_clip_for_channels(r["id"], r["creator_id"], r["channel_id"])


@app.on_event("startup")
def auto_resume_creators() -> None:
    backfill_clip_uploads()
    threading.Thread(target=upload_queue_worker, daemon=True).start()
    threading.Thread(target=archive_worker, daemon=True).start()
    conn = get_db()
    rows = conn.execute("SELECT id FROM creators WHERE auto_record = 1").fetchall()
    conn.close()
    for r in rows:
        cid = r["id"]
        rt = get_runtime(cid)
        if not rt.is_recording:
            threading.Thread(target=creator_worker, args=(cid,), daemon=True).start()


# ---------------------------------------------------------------------------
# API Schemas & Endpoints
# ---------------------------------------------------------------------------
class YouTubeChannelCreate(BaseModel):
    name: str
    handle_or_url: str
    default_privacy: str = "private"
    client_secrets_path: str = ""
    default_hashtags: str = "#shorts #clips"
    auto_upload: bool = False


class CreatorCreate(BaseModel):
    name: str
    platform: str = "twitch"
    url: str
    target_channel_id: str = ""
    auto_record: bool = True
    vertical_9_16: bool = True
    burn_captions: bool = True
    min_score: int = 70


class VodClipRequest(BaseModel):
    creator_id: str
    url: str
    max_clips: int = 3
    clip_duration: int = 40
    vertical_9_16: bool = True
    burn_captions: bool = True


class SettingsUpdate(BaseModel):
    gemini_api_key: Optional[str] = None
    whisper_model: Optional[str] = None
    rclone_remote: Optional[str] = None
    auto_approve_enabled: Optional[bool] = None
    auto_approve_score: Optional[int] = None
    prefilter_enabled: Optional[bool] = None
    prefilter_loudness: Optional[int] = None
    trigger_db_delta: Optional[float] = None
    daily_upload_cap: Optional[int] = None
    project_daily_cap: Optional[int] = None
    upload_min_gap_sec: Optional[int] = None
    queue_expiry_min: Optional[int] = None


class ClipUpdate(BaseModel):
    status: Optional[str] = None
    channel_id: Optional[str] = None


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))


@app.get("/api/health")
def health() -> Dict[str, Any]:
    api_key = get_setting("gemini_api_key", os.environ.get("GEMINI_API_KEY", ""))
    return {
        "whisper_ready": check_whisper_installed(),
        "ai_key_found": bool(api_key.strip()),
        "streamlink_ready": check_binary(["streamlink", "--version"]),
        "yt_dlp_ready": check_binary(["yt-dlp", "--version"]),
        "ffmpeg_ready": check_binary(["ffmpeg", "-version"]),
        "rclone_ready": check_binary(["rclone", "version"]),
        "whisper_model": get_setting("whisper_model", "tiny"),
        "rclone_remote": get_setting("rclone_remote", ""),
        "auto_approve_enabled": get_setting("auto_approve_enabled", "0") == "1",
        "auto_approve_score": int(get_setting("auto_approve_score", "80") or "80"),
        "prefilter_enabled": get_setting("prefilter_enabled", "1") == "1",
        "prefilter_loudness": int(get_setting("prefilter_loudness", "45") or "45"),
        "trigger_db_delta": float(get_setting("trigger_db_delta", "5") or "5"),
        **{f"queue_{k}": v for k, v in queue_config().items()},
    }


@app.post("/api/settings")
def update_settings(body: SettingsUpdate) -> Dict[str, str]:
    if body.gemini_api_key is not None and body.gemini_api_key.strip():
        set_setting("gemini_api_key", body.gemini_api_key.strip())
    if body.whisper_model is not None:
        set_setting("whisper_model", body.whisper_model.strip())
    if body.rclone_remote is not None:
        set_setting("rclone_remote", body.rclone_remote.strip())
    if body.auto_approve_enabled is not None:
        set_setting("auto_approve_enabled", "1" if body.auto_approve_enabled else "0")
    if body.auto_approve_score is not None:
        set_setting("auto_approve_score", str(body.auto_approve_score))
    if body.prefilter_enabled is not None:
        set_setting("prefilter_enabled", "1" if body.prefilter_enabled else "0")
    if body.prefilter_loudness is not None:
        set_setting("prefilter_loudness", str(body.prefilter_loudness))
    for key in ("trigger_db_delta", "daily_upload_cap", "project_daily_cap", "upload_min_gap_sec", "queue_expiry_min"):
        val = getattr(body, key)
        if val is not None:
            set_setting(key, str(val))
    return {"status": "saved"}


@app.post("/api/test-gemini")
def test_gemini() -> Dict[str, Any]:
    api_key = get_setting("gemini_api_key", os.environ.get("GEMINI_API_KEY", "")).strip()
    if not api_key:
        return {"ok": False, "message": "No Gemini API key saved yet."}
    last_err = "Unknown error"
    for model_name in get_gemini_models(api_key):
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        try:
            with httpx.Client(timeout=15.0) as client:
                r = client.post(url, json={"contents": [{"parts": [{"text": "Reply with OK"}]}]})
                if r.status_code == 200:
                    return {"ok": True, "message": f"Connected! Using {model_name}"}
                last_err = f"{model_name} HTTP {r.status_code}: {r.text[:160]}"
        except Exception as exc:
            last_err = str(exc)
    return {"ok": False, "message": last_err}


@app.get("/api/youtube-channels")
def list_youtube_channels() -> List[Dict[str, Any]]:
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM youtube_channels ORDER BY created_at DESC").fetchall()]
    conn.close()
    for r in rows:
        r["authorized"] = (DATA_DIR / f"yt_token_{r['id']}.json").exists() or (DATA_DIR / "yt_profiles" / r["id"]).exists()
        r["studio_login"] = (DATA_DIR / "yt_profiles" / r["id"]).exists()
    return rows


@app.post("/api/youtube-channels")
def create_youtube_channel(body: YouTubeChannelCreate) -> Dict[str, Any]:
    chid = uuid.uuid4().hex[:8]
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_db()
    conn.execute(
        """
        INSERT INTO youtube_channels(id, name, handle_or_url, default_privacy, client_secrets_path, default_hashtags, auto_upload, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (chid, body.name, body.handle_or_url, body.default_privacy, body.client_secrets_path, body.default_hashtags, int(body.auto_upload), now),
    )
    conn.commit()
    conn.close()
    return {"id": chid, "name": body.name}


@app.post("/api/youtube-channels/{channel_id}/authorize")
def authorize_youtube_channel(channel_id: str) -> Dict[str, Any]:
    conn = get_db()
    row = conn.execute("SELECT * FROM youtube_channels WHERE id = ?", (channel_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Channel not found")
    try:
        get_youtube_service(dict(row), allow_interactive_auth=True)
        return {"ok": True, "message": "Channel authorized!"}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))






@app.post("/api/youtube-channels/{channel_id}/studio-login")
def studio_login_endpoint(channel_id: str) -> Dict[str, Any]:
    profile_dir = DATA_DIR / "yt_profiles" / channel_id
    threading.Thread(target=open_yt_studio_login, args=(profile_dir,), daemon=True).start()
    return {"ok": True, "message": "Browser window opening! Sign in to YouTube Studio for this channel, then close that browser window."}

@app.delete("/api/youtube-channels/{channel_id}")
def delete_youtube_channel(channel_id: str) -> Dict[str, str]:
    conn = get_db()
    conn.execute("DELETE FROM youtube_channels WHERE id = ?", (channel_id,))
    conn.execute("UPDATE clip_uploads SET status = 'cancelled' WHERE channel_id = ? AND status = 'pending'", (channel_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}


@app.post("/api/clips/{clip_id}/upload-youtube")
def upload_clip_endpoint(clip_id: str, channel_id: str = "") -> Dict[str, Any]:
    try:
        vid_id = upload_clip_to_youtube(clip_id, channel_id)
        return {"ok": True, "youtube_video_id": vid_id, "url": f"https://youtube.com/shorts/{vid_id}"}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/creators")
def list_creators() -> List[Dict[str, Any]]:
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM creators ORDER BY created_at DESC").fetchall()]
    conn.close()
    for r in rows:
        rt = get_runtime(r["id"])
        r["is_live"] = rt.is_live
        r["is_recording"] = rt.is_recording
    return rows


@app.post("/api/creators")
def create_creator(body: CreatorCreate) -> Dict[str, Any]:
    cid = uuid.uuid4().hex[:8]
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_db()
    conn.execute(
        """
        INSERT INTO creators(id, name, platform, url, target_channel_id, auto_record, vertical_9_16, burn_captions, min_score, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (cid, body.name, body.platform, body.url, body.target_channel_id, int(body.auto_record), int(body.vertical_9_16), int(body.burn_captions), body.min_score, now),
    )
    conn.commit()
    conn.close()
    if body.auto_record:
        threading.Thread(target=creator_worker, args=(cid,), daemon=True).start()
    return {"id": cid, "name": body.name}




@app.delete("/api/creators/{creator_id}")
def delete_creator(creator_id: str) -> Dict[str, str]:
    rt = get_runtime(creator_id)
    rt.stop_event.set()
    rt.is_recording = False
    conn = get_db()
    conn.execute("DELETE FROM creators WHERE id = ?", (creator_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}

@app.post("/api/creators/{creator_id}/start")
def start_creator(creator_id: str) -> Dict[str, str]:
    rt = get_runtime(creator_id)
    if rt.is_recording:
        return {"status": "already_running"}
    threading.Thread(target=creator_worker, args=(creator_id,), daemon=True).start()
    return {"status": "started"}


@app.post("/api/creators/{creator_id}/stop")
def stop_creator(creator_id: str) -> Dict[str, str]:
    rt = get_runtime(creator_id)
    rt.stop_event.set()
    rt.is_recording = False
    return {"status": "stopping"}


CLIP_LIST_COLS = (
    "id, creator_id, channel_id, source_type, title, description, reason, hashtags, tags, score, duration_sec, "
    "aspect, file_path, status, youtube_video_id, synced_drive, created_at, upload_attempts, upload_error, archived, risk_flags"
)


@app.get("/api/creators/{creator_id}/status")
def creator_status(creator_id: str) -> Dict[str, Any]:
    rt = get_runtime(creator_id)
    elapsed = int(time.time() - rt.started_at) if (rt.is_recording and rt.started_at) else 0
    mins, secs = divmod(elapsed, 60)
    conn = get_db()
    clips = [
        dict(r)
        for r in conn.execute(
            f"SELECT {CLIP_LIST_COLS} FROM clips WHERE creator_id = ? ORDER BY created_at DESC LIMIT 60", (creator_id,)
        ).fetchall()
    ]
    total_clips = conn.execute("SELECT COUNT(*) FROM clips WHERE creator_id = ?", (creator_id,)).fetchone()[0]
    conn.close()
    return {
        "is_live": rt.is_live,
        "is_recording": rt.is_recording,
        "is_restreaming": RESTREAMER.is_running(creator_id),
        "stream_title": rt.stream_title,
        "recorded_str": f"{mins}:{secs:02d}",
        "buffer_minutes": rt.buffer_minutes,
        "ai_checks": rt.ai_checks,
        "clips_so_far": total_clips,
        "loudness_history": rt.loudness_history,
        "chat_per_sec": chat_bins(rt),
        "last_transcript": rt.last_transcript,
        "activity_log": rt.activity_log,
        "clips": clips,
    }




class RestreamRequest(BaseModel):
    rtmp_key: str
    overlay_text: str = "GTA VI LIVE"
    zoom_factor: float = 1.06
    pitch_shift: bool = True


@app.post("/api/creators/{creator_id}/restream/start")
def start_restream(creator_id: str, body: RestreamRequest) -> Dict[str, Any]:
    conn = get_db()
    row = conn.execute("SELECT url FROM creators WHERE id = ?", (creator_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Creator not found")
    stream_url = resolve_live_stream_url(row["url"]) or ""
    ok, msg = RESTREAMER.start(
        creator_id=creator_id,
        creator_url=row["url"],
        direct_stream_url=stream_url,
        rtmp_key=body.rtmp_key.strip(),
        overlay_text=body.overlay_text,
        zoom_factor=body.zoom_factor,
        pitch_shift=body.pitch_shift,
        log_dir=LOGS_DIR,
    )
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"status": "restreaming", "message": msg}


@app.post("/api/creators/{creator_id}/restream/stop")
def stop_restream(creator_id: str) -> Dict[str, str]:
    RESTREAMER.stop(creator_id)
    return {"status": "stopped"}

@app.post("/api/creators/{creator_id}/manual-clip")
def manual_clip(creator_id: str, seconds: int = 40) -> Dict[str, Any]:
    """Clip the last `seconds` of the live buffer. Only finished segments are used (the newest file is
    still being written and cannot be read), so the clip ends up to ~SEG_SECONDS before 'now'."""
    seconds = max(10, min(int(seconds), 120))
    conn = get_db()
    c_row = conn.execute("SELECT * FROM creators WHERE id = ?", (creator_id,)).fetchone()
    conn.close()
    if not c_row:
        raise HTTPException(status_code=404, detail="Creator not found")
    creator = dict(c_row)

    c_buf = TEMP_DIR / creator_id
    all_segs = sorted(c_buf.glob("seg_*.mp4"))
    completed = all_segs[:-1] if len(all_segs) > 1 else []
    if not completed:
        raise HTTPException(status_code=400, detail="No finished video segments buffered yet (wait ~10 seconds).")

    need = int(seconds // SEG_SECONDS) + 2  # +2 so a keyframe-snapped cut still has enough lead-in
    use = completed[-need:]
    merged = c_buf / f"manual_src_{uuid.uuid4().hex[:6]}.mp4"
    try:
        if not concat_chunks(use, merged):
            raise HTTPException(status_code=500, detail="Could not join buffered segments.")
        total = get_duration_sec(merged)
        dur = float(min(seconds, total)) if total > 0 else float(seconds)
        start = max(0.0, total - dur) if total > 0 else 0.0

        all_segs_t = transcribe_segment(merged)
        segs = shift_segments(all_segs_t, start, dur)
        text = " ".join(s["text"] for s in segs) if segs else "Manual clip"
        ai_res = score_moment(text, 75.0, 0, creator["name"], get_runtime(creator_id).stream_title)
        out_path = CLIPS_DIR / f"{creator_id}_manual_{uuid.uuid4().hex[:6]}.mp4"
        ok = render_clip(
            merged,
            out_path,
            vertical_9_16=bool(creator["vertical_9_16"]),
            burn_captions=bool(creator["burn_captions"]),
            segments=segs,
            start_sec=start,
            duration_sec=dur,
        )
    finally:
        merged.unlink(missing_ok=True)
    if not ok:
        raise HTTPException(status_code=500, detail="ffmpeg failed to render clip.")
    return save_clip_record(
        creator_id=creator_id,
        channel_id=creator.get("target_channel_id", ""),
        source_type="live",
        title=ai_res["title"],
        description=ai_res.get("description", ""),
        reason=ai_res["reason"],
        hashtags=ai_res["hashtags"],
        tags=ai_res.get("tags", []),
        score=max(85, ai_res["score"]),
        duration_sec=int(round(dur)),
        aspect="9:16" if creator["vertical_9_16"] else "16:9",
        file_path=out_path,
        transcript=text,
        risk_flags=ai_res.get("risk_flags", []),
    )


@app.post("/api/vod/process")
def start_vod_job(body: VodClipRequest) -> Dict[str, str]:
    job_id = uuid.uuid4().hex[:8]
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_db()
    conn.execute(
        "INSERT INTO vod_jobs(id, creator_id, url, status, progress, created_at) VALUES(?, ?, ?, 'queued', 'Queued...', ?)",
        (job_id, body.creator_id, body.url, now),
    )
    conn.commit()
    conn.close()
    threading.Thread(
        target=vod_worker,
        args=(job_id, body.creator_id, body.url, body.max_clips, body.clip_duration, body.vertical_9_16, body.burn_captions),
        daemon=True,
    ).start()
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/vod/jobs")
def list_vod_jobs() -> List[Dict[str, Any]]:
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM vod_jobs ORDER BY created_at DESC LIMIT 15").fetchall()]
    conn.close()
    return rows


@app.get("/api/clips")
def list_all_clips() -> List[Dict[str, Any]]:
    conn = get_db()
    rows = [dict(r) for r in conn.execute(f"SELECT {CLIP_LIST_COLS} FROM clips ORDER BY created_at DESC LIMIT 300").fetchall()]
    conn.close()
    return rows


@app.get("/api/upload-queue")
def upload_queue_view() -> Dict[str, Any]:
    now_ts = time.time()
    cfg = queue_config()
    conn = get_db()
    rows = conn.execute(
        """
        SELECT u.id, u.clip_id, u.channel_id, u.status, u.attempts, u.error, u.created_at AS queued_at, c.title, c.score
        FROM clip_uploads u JOIN clips c ON c.id = u.clip_id
        WHERE u.status = 'pending' ORDER BY u.created_at DESC LIMIT 100
        """
    ).fetchall()
    used = conn.execute(
        "SELECT channel_id, COUNT(*) AS n FROM clip_uploads WHERE status = 'uploaded' AND uploaded_at > ? GROUP BY channel_id",
        (now_ts - 86400,),
    ).fetchall()
    conn.close()
    pending = []
    for r in rows:
        d = dict(r)
        try:
            age = max(0.0, (now_ts - datetime.fromisoformat(d["queued_at"]).timestamp()) / 60.0)
        except ValueError:
            age = 0.0
        d["rank"] = round(float(d["score"] or 0) - FRESHNESS_PENALTY_PER_MIN * age, 1)
        pending.append(d)
    pending.sort(key=lambda x: x["rank"], reverse=True)
    return {
        "config": cfg,
        "uploaded_last_24h": {r["channel_id"]: r["n"] for r in used},
        "paused_channels": {k: int(v - now_ts) for k, v in PAUSED_UNTIL.items() if v > now_ts},
        "pending_ranked": pending,
    }


@app.patch("/api/clips/{clip_id}")
def update_clip(clip_id: str, body: ClipUpdate) -> Dict[str, str]:
    if body.status is not None and body.status not in ("approved", "rejected", "pending"):
        raise HTTPException(status_code=400, detail="status must be approved, rejected or pending")
    conn = get_db()
    if body.channel_id is not None:
        conn.execute("UPDATE clips SET channel_id = ? WHERE id = ?", (body.channel_id, clip_id))
        conn.execute("DELETE FROM clip_uploads WHERE clip_id = ? AND status = 'pending'", (clip_id,))
        conn.commit()
    if body.status is not None:
        conn.execute("UPDATE clips SET status = ? WHERE id = ?", (body.status, clip_id))
        if body.status in ("rejected", "pending"):
            conn.execute("UPDATE clip_uploads SET status = 'cancelled' WHERE clip_id = ? AND status = 'pending'", (clip_id,))
        conn.commit()
        if body.status == "approved":
            row_c = conn.execute("SELECT creator_id, channel_id FROM clips WHERE id = ?", (clip_id,)).fetchone()
            if row_c:
                enqueue_clip_for_channels(clip_id, row_c["creator_id"], row_c["channel_id"])
    conn.commit()
    conn.close()
    return {"status": "updated"}


@app.post("/api/open-folder")
def open_clips_folder(which: str = "finished") -> Dict[str, str]:
    path = str(UPLOADED_DIR if which == "uploaded" else CLIPS_DIR)
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception:
        pass
    return {"path": path}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8765, reload=False)
