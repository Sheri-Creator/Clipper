# Clipper v3 - what changed and what to do first

## Do this before the first run
1. Close the old app. Delete `data/yt_profiles/` (the saved Google/Studio browser login is no longer used).
2. In "My YouTube Channels" click **Authorize (API)** for the channel you upload to.
   If your Google OAuth consent screen is in "Testing" mode, the login expires after ~7 days
   and the channel will show an "expired or revoked" error: click Authorize again (or publish the consent screen).
3. Settings: leave **Auto-Approve off** while GTA VI launches. Approve clips yourself in the creator view.
4. New channels default to **Private** so you can look at an upload in Studio before it goes public.
   Change the default per channel when you trust the output.
5. `pip install -r requirements.txt` again (Playwright is gone, so `playwright install` is no longer needed).

## 1. API-only uploads
- `yt_studio_uploader.py`, the Studio login button and the Chromium verifier loop are removed.
- Uploads use the YouTube Data API with the saved OAuth login.
- No pinned channel on a creator: clips go to the single auto-upload channel if there is exactly one,
  otherwise they are NOT queued (v2 sent them to every auto-upload channel).
- Comma-separated channel lists now work for the manual Upload button (uses the first channel) and the queue.
- A clip is never uploaded twice to the same channel.
- Daily upload/quota errors pause that channel for 1 h without burning retry attempts.
  Auth errors pause it for 10 min. After 5 real failures the row becomes `failed`.
- Approved clips that never had queue rows (approved under v1) are queued at startup if recent.

## 2. Silent-failure fixes
- Recorder wipes its buffer folder every time ffmpeg starts, so old `seg_*` files can't be mistaken for new ones.
- Ring-buffer delete is wrapped so a Windows file lock can't kill the worker thread.
- Stop then Start quickly no longer leaves two recorders/chat listeners running.
- Twitch chat: parses line by line, always answers PING, reconnects with backoff (tested against a fake IRC server).
- "Clip last N sec" only uses finished segments (the newest file is still being written), cuts exactly the last N sec
  of what is available, and works for 10-120 s. It ends up to ~5 s before "now".

## 3. Review gate and no-spoiler rule
- Auto-approve defaults to OFF.
- A clip is auto-approved only if: auto-approve is on, score >= threshold, AND it has no spoiler flag.
- Flags come from (a) Gemini `spoiler_risk`, (b) keyword scan of title/description/transcript
  (cutscene, ending, credits, plot, twist, leak, final mission, ...). Flagged clips show a red warning in the UI.
- The Gemini prompt forbids titles that reveal plot, endings, deaths, mission outcomes or leaked content.
- Not enforced by code: whether a clip contains publisher-restricted material (cutscenes, leaks). You still have to watch it.

## 4. Speed
- Loudness check skips video decoding (`-vn`): ~4x faster in my test (0.225 s -> 0.054 s per segment).
- Blurred background is built at quarter resolution: 2.5x faster render in my test (43 s -> 17.5 s for a 25 s clip, 1 CPU core).
- Segments are 5 s (they snap to keyframes, so 4-6 s on Twitch), not 30 s, so there is no more 30-60 s wait before analysis.

## 5. Ranked queue + peak-centred cuts
- Peak detection is per creator: a segment counts when it is `Peak sensitivity` dB (default 5) above that
  stream's own recent median loudness, or chat is >=2.5x its own baseline (and at least 8 msgs / 5 s).
  First minute uses the old absolute "Min Loud" as a fallback. Prefilter off = analyse the best moment every ~30 s.
- Clip = 10 s before the peak segment's centre to 15 s after (~25 s). Constants: `CLIP_BEFORE`, `CLIP_AFTER` in app.py.
- Upload queue is ranked by `score - 0.25 x minutes waiting`; best+freshest goes first.
- Budgets (Settings): per channel per 24 h (default 30), all channels per 24 h (default 90), min seconds between
  uploads per channel (default 300), queue expiry (default 180 min, counted from approval).
- `GET /api/upload-queue` shows the ranked queue, 24 h usage and paused channels.

## Also changed (cheap wins)
- GTA 6 / GTA VI added to tags + hashtags (AI and fallback) when the transcript/stream title mentions it.
- Gemini: model list cached 1 h, and a 120 s cooldown to local scoring when every model returns 429.
- CORS limited to localhost:8765. AI text is HTML-escaped in the UI. Video previews no longer restart every 4 s.
- Status/clips endpoints no longer ship every transcript.

## Not done
- Studio/OAuth flow, Gemini calls and real Whisper were not exercised here (no network, no Google account).
- Thresholds (5 dB, chat x2.5) are starting points: watch the log for "quiet segments skipped" and adjust.
- Frame is still blurred-background 16:9 in 9:16 (clip layout unchanged).
- VOD download still times out at 10 min.
