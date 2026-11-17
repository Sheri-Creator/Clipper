import logging
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger("clipper.restream")


class RestreamManager:
    """
    High-stability YouTube Live restreamer (🟢 Excellent stream health):
    - Uses Streamlink multi-threaded segment fetching (--stream-segment-threads 3 --ringbuffer-size 32M)
    - Uses FFmpeg -preset ultrafast -tune zerolatency with -thread_queue_size 1024 so encoding never drops below 1.0x
    - Uses zero-drift audio EQ fingerprint transform instead of asetrate/atempo clock-skewing filters
    - Centers top banner text so corner badges never overlap
    """

    def __init__(self) -> None:
        self.procs: Dict[str, Tuple[Optional[subprocess.Popen], subprocess.Popen]] = {}
        self.last_error: Dict[str, str] = {}
        self.lock = threading.Lock()

    def _build_vf(self, overlay_text: str, zoom_factor: float, use_drawtext: bool = True) -> str:
        z = max(1.02, min(1.25, zoom_factor))
        vf = (
            "scale=1280:720:force_original_aspect_ratio=increase,crop=1280:720,"
            f"scale=trunc(iw*{z:.2f}/2)*2:trunc(ih*{z:.2f}/2)*2,crop=1280:720,"
            "drawbox=x=0:y=0:w=1280:h=44:color=black@0.75:t=fill"
        )
        safe_txt = "".join(c for c in overlay_text if c.isalnum() or c in " -_!#")[:42].strip()
        if use_drawtext and safe_txt:
            if os.name == "nt" and Path("C:/Windows/Fonts/arialbd.ttf").exists():
                vf += f",drawtext=fontfile='C\\:/Windows/Fonts/arialbd.ttf':text='{safe_txt}':fontcolor=white:fontsize=22:x=(w-text_w)/2:y=11"
            elif Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf").exists():
                vf += f",drawtext=fontfile='/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf':text='{safe_txt}':fontcolor=white:fontsize=22:x=(w-text_w)/2:y=11"
        return vf

    def start(
        self,
        creator_id: str,
        creator_url: str,
        direct_stream_url: str,
        rtmp_key: str,
        overlay_text: str = "GTA VI LIVE",
        zoom_factor: float = 1.06,
        pitch_shift: bool = True,
        log_dir: Optional[Path] = None,
    ) -> Tuple[bool, str]:
        with self.lock:
            self._stop_unlocked(creator_id)
            key_clean = rtmp_key.strip()
            rtmp_url = key_clean if key_clean.startswith("rtmp") else f"rtmp://a.rtmp.youtube.com/live2/{key_clean}"
            # Zero-clock-drift audio transform (keeps exact 1.000x frame pacing for YouTube Studio)
            af = (
                "highpass=f=70,equalizer=f=320:width_type=o:width=2:g=2.5,"
                "equalizer=f=3200:width_type=o:width=2:g=-2.0,volume=1.03"
                if pitch_shift
                else "anull"
            )
            err_path = (log_dir or Path(".")) / f"restream_{creator_id}.log"

            for use_dt in (True, False):
                vf = self._build_vf(overlay_text, zoom_factor, use_drawtext=use_dt)
                sl_proc: Optional[subprocess.Popen] = None
                err_fh = open(err_path, "w", encoding="utf-8", errors="ignore")

                if shutil.which("streamlink"):
                    sl_proc = subprocess.Popen(
                        [
                            "streamlink",
                            "--stdout",
                            "--stream-segment-threads",
                            "3",
                            "--ringbuffer-size",
                            "32M",
                            "--hls-live-edge",
                            "4",
                            creator_url,
                            "720p,720p60,480p,best",
                        ],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                    )
                    stdin_pipe = sl_proc.stdout
                else:
                    stdin_pipe = None

                cmd = ["ffmpeg", "-y", "-fflags", "+genpts", "-thread_queue_size", "1024"]
                if stdin_pipe is not None:
                    cmd.extend(["-i", "pipe:0"])
                else:
                    cmd.extend(["-re", "-i", direct_stream_url])

                cmd.extend(
                    [
                        "-vf",
                        vf,
                        "-af",
                        af,
                        "-c:v",
                        "libx264",
                        "-preset",
                        "ultrafast",
                        "-tune",
                        "zerolatency",
                        "-b:v",
                        "2200k",
                        "-minrate",
                        "2200k",
                        "-maxrate",
                        "2200k",
                        "-bufsize",
                        "4400k",
                        "-pix_fmt",
                        "yuv420p",
                        "-r",
                        "30",
                        "-vsync",
                        "cfr",
                        "-g",
                        "60",
                        "-keyint_min",
                        "60",
                        "-sc_threshold",
                        "0",
                        "-c:a",
                        "aac",
                        "-b:a",
                        "128k",
                        "-ar",
                        "44100",
                        "-f",
                        "flv",
                        rtmp_url,
                    ]
                )

                ff_proc = subprocess.Popen(cmd, stdin=stdin_pipe, stdout=subprocess.DEVNULL, stderr=err_fh)
                time.sleep(3.0)
                if ff_proc.poll() is None:
                    self.procs[creator_id] = (sl_proc, ff_proc)
                    self.last_error[creator_id] = ""
                    logger.info("Restreaming %s -> %s (ultrafast CFR 30fps)", creator_url, rtmp_url)
                    return True, "Live restream started! Stream health should turn Green in ~10s."

                err_fh.close()
                if sl_proc and sl_proc.poll() is None:
                    sl_proc.terminate()
                err_txt = err_path.read_text(encoding="utf-8", errors="ignore")[-350:].strip()
                self.last_error[creator_id] = err_txt

            return False, f"FFmpeg restream exited early: {self.last_error.get(creator_id, 'Unknown error')}"

    def _stop_unlocked(self, creator_id: str) -> None:
        pair = self.procs.pop(creator_id, None)
        if pair:
            sl_proc, ff_proc = pair
            for p in (ff_proc, sl_proc):
                if p and p.poll() is None:
                    try:
                        p.terminate()
                    except Exception:
                        pass

    def stop(self, creator_id: str) -> None:
        with self.lock:
            self._stop_unlocked(creator_id)

    def is_running(self, creator_id: str) -> bool:
        pair = self.procs.get(creator_id)
        if not pair:
            return False
        _, ff_proc = pair
        return ff_proc.poll() is None


RESTREAMER = RestreamManager()
