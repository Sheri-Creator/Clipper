import logging
import threading
import time
from pathlib import Path
from typing import List

logger = logging.getLogger("clipper.yt_studio")
YT_STUDIO_LOCK = threading.Lock()


def open_yt_studio_login(profile_dir: Path) -> None:
    """Opens a visible Chromium window so the user can log in to YouTube Studio once."""
    from playwright.sync_api import sync_playwright

    profile_dir.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto("https://studio.youtube.com", timeout=60000)
        # Wait until user finishes logging in and closes the browser window
        while len(ctx.pages) > 0:
            try:
                time.sleep(1)
            except Exception:
                break
        ctx.close()


def upload_via_yt_studio(
    profile_dir: Path,
    video_path: Path,
    title: str,
    description: str,
    tags: List[str],
    privacy: str = "public",
) -> str:
    """
    Uploads an approved clip via YouTube Studio using the saved browser session.
    Returns the uploaded YouTube video URL/ID when finished.
    """
    from playwright.sync_api import sync_playwright

    if not profile_dir.exists():
        raise RuntimeError("Channel not logged into YT Studio yet. Click 'Login YT Studio' first.")
    if not video_path.exists():
        raise RuntimeError(f"Video file missing: {video_path}")

    with YT_STUDIO_LOCK:
        with sync_playwright() as p:
            ctx = p.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            try:
                page = ctx.pages[0] if ctx.pages else ctx.new_page()
                page.goto("https://studio.youtube.com", timeout=60000)
                page.wait_for_load_state("networkidle", timeout=30000)

                if "accounts.google.com" in page.url:
                    raise RuntimeError("Session expired. Click 'Login YT Studio' to sign in again.")

                # Click Create -> Upload videos
                create_btn = page.locator("ytcp-button#create-icon, #create-icon").first
                create_btn.click(timeout=15000)
                upload_menu = page.locator("tp-yt-paper-item#text-item-0, ytcp-text-menu tp-yt-paper-item").first
                upload_menu.click(timeout=10000)

                # Attach MP4 file
                file_input = page.locator("input[type='file']").first
                file_input.set_input_files(str(video_path.resolve()), timeout=15000)

                # Wait for Details dialog & fill Title + Description
                title_box = page.locator("#textbox[aria-label*='title' i], ytcp-social-suggestions-textbox#title-textarea #textbox").first
                title_box.wait_for(state="visible", timeout=30000)
                title_box.click()
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                title_box.fill(title[:98])

                desc_box = page.locator("ytcp-social-suggestions-textbox#description-textarea #textbox, #description-textarea #textbox").first
                if desc_box.count() > 0:
                    desc_box.click()
                    desc_box.fill(description[:4500])

                # Select "No, it's not made for kids"
                not_kids = page.locator("tp-yt-paper-radio-button[name='VIDEO_MADE_FOR_KIDS_NOT_MFK']").first
                if not_kids.count() > 0:
                    not_kids.click(timeout=10000)

                # Expand "Show more" to add tags if available
                show_more = page.locator("ytcp-button#toggle-button").first
                if show_more.count() > 0 and tags:
                    try:
                        show_more.click(timeout=5000)
                        tag_input = page.locator("input[aria-label='Tags']").first
                        if tag_input.count() > 0:
                            tag_input.fill(", ".join(tags[:12]))
                            page.keyboard.press("Enter")
                    except Exception:
                        pass

                # Next -> Next -> Next (Details -> Video elements -> Checks -> Visibility)
                next_btn = page.locator("#next-button").first
                for _ in range(3):
                    next_btn.click(timeout=15000)
                    time.sleep(1.2)

                # Set Visibility (PUBLIC / UNLISTED / PRIVATE)
                vis_map = {"public": "PUBLIC", "unlisted": "UNLISTED", "private": "PRIVATE"}
                vis_name = vis_map.get(privacy.lower(), "PUBLIC")
                vis_radio = page.locator(f"tp-yt-paper-radio-button[name='{vis_name}']").first
                vis_radio.click(timeout=15000)

                # Grab video URL/ID shown on the right panel if present
                video_id = ""
                try:
                    link_el = page.locator("a.style-scope.ytcp-video-info[href*='youtu.be'], a[href*='youtube.com/shorts']").first
                    if link_el.count() > 0:
                        href = link_el.get_attribute("href") or ""
                        video_id = href.rstrip("/").split("/")[-1]
                except Exception:
                    pass

                # Wait for upload to finish (up to 5 min on slow connections) before clicking Save/Publish
                for _ in range(60):
                    prog = page.locator("span.progress-label").first
                    txt = (prog.inner_text() if prog.count() > 0 else "").lower()
                    if "complete" in txt or "finished" in txt or "checks" in txt:
                        break
                    time.sleep(5)
                done_btn = page.locator("#done-button").first
                done_btn.click(timeout=20000)
                time.sleep(4)
                logger.info("YT Studio upload completed: %s (%s)", title, video_id or "published")
                return video_id or "yt_studio_uploaded"
            finally:
                ctx.close()


def verify_title_in_yt_studio(profile_dir: Path, title_prefix: str) -> bool:
    """Checks YouTube Studio Content tab to confirm a video with title_prefix is listed."""
    from playwright.sync_api import sync_playwright

    if not profile_dir.exists():
        return False
    with YT_STUDIO_LOCK:
        with sync_playwright() as p:
            ctx = p.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            try:
                page = ctx.pages[0] if ctx.pages else ctx.new_page()
                page.goto("https://studio.youtube.com", timeout=60000)
                page.wait_for_load_state("networkidle", timeout=30000)
                content_tab = page.locator("#menu-item-1, a[href*='/videos']").first
                if content_tab.count() > 0:
                    content_tab.click(timeout=10000)
                    time.sleep(3)
                body_txt = page.inner_text("body").lower()
                return title_prefix[:25].lower() in body_txt
            except Exception:
                return False
            finally:
                ctx.close()
