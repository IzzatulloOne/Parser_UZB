from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, Tag

if TYPE_CHECKING:
    import pandas as pd


BASE_URL = "https://platesmania.com"
DEFAULT_OUTPUT = Path("dataset") / "platesmania_links.csv"
DEFAULT_DEBUG_DIR = Path("dataset") / "_debug"
DEFAULT_HEADERS_ROTATE_EVERY = 8
DEFAULT_DELAY_MIN = 1.8
DEFAULT_DELAY_MAX = 4.0
DEFAULT_SEGMENT_CONCURRENCY = 2
DEFAULT_MAX_PAGES = 250

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

REGION_CONFIG = [
    {"name": "tashkent",        "title": "Ташкент",             "r_id": 102, "mask": "01"},
    {"name": "tashkent_region", "title": "Ташкентская область", "r_id": 103, "mask": "10"},
    {"name": "fergana",         "title": "Фергана",             "r_id": 115, "mask": "40"},
    {"name": "andijan",         "title": "Андижан",             "r_id": 111, "mask": "60"},
    {"name": "namangan",        "title": "Наманган",            "r_id": 112, "mask": "50"},
    {"name": "samarkand",       "title": "Самарканд",           "r_id": 108, "mask": "30"},
    {"name": "bukhara",         "title": "Бухара",              "r_id": 109, "mask": "80"},
    {"name": "navoi",           "title": "Навои",               "r_id": 110, "mask": "85"},
    {"name": "kashkadarya",     "title": "Кашкадарья",          "r_id": 107, "mask": "70"},
    {"name": "surkhandarya",    "title": "Сурхандарья",         "r_id": 114, "mask": "75"},
    {"name": "khorezm",         "title": "Хорезм",              "r_id": 113, "mask": "90"},
    {"name": "jizzakh",         "title": "Джизак",              "r_id": 106, "mask": "25"},
    {"name": "syrdarya",        "title": "Сырдарья",            "r_id": 104, "mask": "20"},
    {"name": "karakalpakstan",  "title": "Каракалпакстан",      "r_id": 105, "mask": "95"},
]

CTYPE_LABELS = {1: "private", 2: "legal"}

IMAGE_HINT_RE = re.compile(
    r"https?://img\d+\.platesmania\.com/[^\s\"'>]+?\.(?:jpg|jpeg|png|webp)",
    re.IGNORECASE,
)
IMAGE_ALT_RE  = re.compile(r"image\s*:\s*([A-Z0-9А-ЯІЇЄҐ ]{5,24})", re.IGNORECASE)
TEXT_PLATE_RE = re.compile(r"\b(?:[0-9A-Z]{1,4}\s+){1,4}[0-9A-Z]{1,4}\b")

LOGGER = logging.getLogger("platesmania_scanner")
SAVED_COOKIES_PATH = Path("dataset") / "session_cookies.json"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Segment:
    region_name: str
    region_title: str
    region_id: int
    mask: str
    ctype: int

    @property
    def label(self) -> str:
        return f"{self.region_name}/ctype={self.ctype}/mask={self.mask}*"

    def page_url(self, page: int) -> str:
        return (
            f"{BASE_URL}/uz/gallery-{page}"
            f"?&r={self.region_id}&nomer={self.mask}*&ctype={self.ctype}"
        )


@dataclass(frozen=True)
class PlateRecord:
    filename: str
    image_url: str
    plate_text: str
    plate_display: str
    source_page: int
    source_url: str
    region_name: str
    region_title: str
    region_id: int
    mask: str
    ctype: int
    ctype_label: str


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class AntiBotError(RuntimeError):
    """PlatesMania returned KillBot verification page."""


# ---------------------------------------------------------------------------
# ChromeDriver cookie harvester
# ---------------------------------------------------------------------------

def harvest_cookies_via_browser(driver_path: str | None = None) -> dict[str, str]:
    """
    Opens a real Chrome window so the user can pass KillBot manually.
    After the user presses ENTER, extracts all cookies and saves them
    to dataset/session_cookies.json for future reuse.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
    except ImportError:
        LOGGER.error(
            "selenium not installed. Run:  pip install selenium\n"
            "Or pass cookies via --cookies-file / --cookie-header."
        )
        raise

    LOGGER.info("=" * 60)
    LOGGER.info("Opening Chrome for manual captcha solving...")
    LOGGER.info("  1. Solve any captcha / verification that appears")
    LOGGER.info("  2. Make sure the gallery page is fully loaded")
    LOGGER.info("  3. Press ENTER in this terminal to continue")
    LOGGER.info("=" * 60)

    options = Options()
    options.add_argument("--start-maximized")
    # Hide automation flags so KillBot doesn't detect Selenium
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    if driver_path:
        driver = webdriver.Chrome(service=Service(executable_path=driver_path), options=options)
    else:
        # Selenium 4.6+ downloads chromedriver automatically
        driver = webdriver.Chrome(options=options)

    # Patch navigator.webdriver JS flag
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )

    try:
        target = f"{BASE_URL}/uz/gallery-1?&r=102&nomer=01*&ctype=1"
        driver.get(target)

        print("\n>>> Browser is open. Solve captcha if needed, then press ENTER here <<<")
        input()

        raw_cookies = driver.get_cookies()
        cookies: dict[str, str] = {c["name"]: c["value"] for c in raw_cookies}

        LOGGER.info("Harvested %d cookies from browser session", len(cookies))

        # Save for reuse — next run won't need to open the browser
        SAVED_COOKIES_PATH.parent.mkdir(parents=True, exist_ok=True)
        SAVED_COOKIES_PATH.write_text(
            json.dumps(raw_cookies, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        LOGGER.info("Cookies saved → %s (reused on next run automatically)", SAVED_COOKIES_PATH)

        return cookies
    finally:
        driver.quit()


# ---------------------------------------------------------------------------
# Header rotation
# ---------------------------------------------------------------------------

class HeaderRotator:
    def __init__(self, rotate_every: int) -> None:
        self.rotate_every = max(1, rotate_every)
        self._count = 0
        self._ua = random.choice(USER_AGENTS)

    def next_headers(self) -> dict[str, str]:
        self._count += 1
        if self._count == 1 or self._count % self.rotate_every == 0:
            self._ua = random.choice(USER_AGENTS)

        chrome = "Chrome" in self._ua
        return {
            "User-Agent": self._ua,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": f"{BASE_URL}/uz/",
            "Upgrade-Insecure-Requests": "1",
            **(
                {
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1",
                    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                }
                if chrome else {}
            ),
        }


# ---------------------------------------------------------------------------
# Async scanner
# ---------------------------------------------------------------------------

class AsyncScanner:
    def __init__(
        self,
        *,
        timeout_seconds: float,
        rotate_every: int,
        delay_min: float,
        delay_max: float,
        cookie_reset_every: int,
        cookies: dict[str, str],
        debug_dir: Path,
        save_debug_on_error: bool,
        max_retries: int = 4,
    ) -> None:
        self.timeout_seconds     = timeout_seconds
        self.delay_min           = delay_min
        self.delay_max           = delay_max
        self.cookie_reset_every  = max(0, cookie_reset_every)
        self.debug_dir           = debug_dir
        self.save_debug_on_error = save_debug_on_error
        self.max_retries         = max_retries
        self.rotator             = HeaderRotator(rotate_every=rotate_every)
        self._client: httpx.AsyncClient | None = None
        self._cookies            = cookies.copy()
        self._lock               = asyncio.Lock()
        self._page_fetch_count   = 0

    async def __aenter__(self) -> "AsyncScanner":
        self._client = self._make_client()
        await self._warmup()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _make_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(self.timeout_seconds),
            headers=self.rotator.next_headers(),
            cookies=self._cookies,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            http2=True,
        )

    async def _warmup(self) -> None:
        """Visit main page first — looks more human."""
        if self._client is None:
            return
        try:
            await self._client.get(f"{BASE_URL}/uz/")
            await asyncio.sleep(random.uniform(1.5, 3.0))
            LOGGER.debug("Warmup done")
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Warmup failed (non-fatal): %s", exc)

    async def _refresh(self) -> None:
        async with self._lock:
            if self._client is None:
                self._client = self._make_client()
                return
            self._page_fetch_count += 1
            self._client.headers.update(self.rotator.next_headers())
            if self.cookie_reset_every and self._page_fetch_count % self.cookie_reset_every == 0:
                LOGGER.info("Resetting cookies after %s requests", self._page_fetch_count)
                self._client.cookies.clear()
                self._client.cookies.update(self._cookies)

    async def fetch_gallery(self, url: str) -> str:
        for attempt in range(self.max_retries):
            await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))
            await self._refresh()

            if self._client is None:
                raise RuntimeError("HTTP client not initialized")

            try:
                response = await self._client.get(url)
            except httpx.TimeoutException:
                wait = (2 ** attempt) * random.uniform(3.0, 6.0)
                LOGGER.warning("Timeout %s  retry %s/%s  wait=%.1fs", url, attempt + 1, self.max_retries, wait)
                await asyncio.sleep(wait)
                continue
            except httpx.RequestError as exc:
                LOGGER.warning("RequestError: %s  retry %s/%s", exc, attempt + 1, self.max_retries)
                await asyncio.sleep(random.uniform(5.0, 10.0))
                continue

            if response.status_code in (429, 503):
                wait = (2 ** attempt) * random.uniform(15.0, 30.0)
                LOGGER.warning("HTTP %s  backing off %.1fs  attempt %s/%s", response.status_code, wait, attempt + 1, self.max_retries)
                await asyncio.sleep(wait)
                continue

            response.raise_for_status()
            html = response.text

            if is_antibot_page(html):
                if self.save_debug_on_error:
                    save_debug_html(self.debug_dir, "killbot_block", html)
                raise AntiBotError(
                    "KillBot verification detected.\n"
                    "  → Run with --use-browser to solve it interactively.\n"
                    "  → Or pass cookies via --cookies-file / --cookie-header."
                )

            return html

        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} retries")


# ---------------------------------------------------------------------------
# Anti-bot helpers
# ---------------------------------------------------------------------------

def is_antibot_page(html: str) -> bool:
    lowered = html.lower()
    return any(m in lowered for m in (
        "killbot user verification",
        "user verification",
        "window.kberrors",
        "id='kb-recaptcha'",
        'id="kb-recaptcha"',
    ))


def save_debug_html(debug_dir: Path, prefix: str, html: str) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    path = debug_dir / f"{prefix}.html"
    path.write_text(html, encoding="utf-8")
    LOGGER.info("Debug HTML → %s", path)


# ---------------------------------------------------------------------------
# Segment builder
# ---------------------------------------------------------------------------

def build_segments(region_filter: set[str] | None, ctypes: list[int]) -> list[Segment]:
    out: list[Segment] = []
    for item in REGION_CONFIG:
        if region_filter and item["name"] not in region_filter:
            continue
        for ctype in ctypes:
            out.append(Segment(
                region_name=item["name"],
                region_title=item["title"],
                region_id=item["r_id"],
                mask=item["mask"],
                ctype=ctype,
            ))
    return out


# ---------------------------------------------------------------------------
# Cookie helpers
# ---------------------------------------------------------------------------

def parse_cookie_input(cookie_header: str | None, cookies_file: Path | None) -> dict[str, str]:
    if cookie_header:
        return _header_to_dict(cookie_header)
    if not cookies_file:
        return {}
    raw = cookies_file.read_text(encoding="utf-8").strip()
    if not raw:
        return {}
    if raw.lstrip().startswith(("{", "[")):
        return _json_to_dict(json.loads(raw))
    return _header_to_dict(raw)


def _json_to_dict(payload: object) -> dict[str, str]:
    if isinstance(payload, dict):
        if "cookies" in payload and isinstance(payload["cookies"], list):
            return {str(i["name"]): str(i["value"]) for i in payload["cookies"] if "name" in i}
        return {str(k): str(v) for k, v in payload.items()}
    if isinstance(payload, list):
        return {str(i["name"]): str(i["value"]) for i in payload if "name" in i}
    raise ValueError("Unsupported cookie JSON format")


def _header_to_dict(value: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in value.split(";"):
        if "=" not in part:
            continue
        name, val = part.split("=", 1)
        name = name.strip()
        if name:
            out[name] = val.strip()
    return out


# ---------------------------------------------------------------------------
# Plate text helpers
# ---------------------------------------------------------------------------

def normalize_plate_display(text: str) -> str:
    text = text.replace("\xa0", " ").strip()
    text = re.sub(r"^image\s*:\s*", "", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).upper()


def normalize_plate_text(text: str) -> str:
    s = normalize_plate_display(text)
    s = re.sub(r"[^0-9A-ZА-ЯІЇЄҐ ]+", "", s)
    return s.replace(" ", "")


def looks_like_plate(text: str) -> bool:
    c = normalize_plate_text(text)
    return (
        6 <= len(c) <= 10
        and sum(ch.isdigit() for ch in c) >= 2
        and sum(ch.isalpha() for ch in c) >= 1
    )


def extract_plate_candidates(text: str) -> list[str]:
    text = text.replace("\xa0", " ").upper()
    candidates: list[str] = []
    for m in IMAGE_ALT_RE.finditer(text):
        c = normalize_plate_display(m.group(1))
        if looks_like_plate(c):
            candidates.append(c)
    for m in TEXT_PLATE_RE.finditer(text):
        c = normalize_plate_display(m.group(0))
        if looks_like_plate(c):
            candidates.append(c)
    seen: set[str] = set()
    unique: list[str] = []
    for c in candidates:
        k = normalize_plate_text(c)
        if k not in seen:
            unique.append(c)
            seen.add(k)
    return unique


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def extract_image_url_from_tag(node: Tag, page_url: str) -> str | None:
    for attr in ("src", "data-src", "data-original", "data-lazy-src", "href"):
        value = node.get(attr)
        if not value:
            continue
        absolute = urljoin(page_url, value)
        if IMAGE_HINT_RE.search(absolute) and "/inf/" not in absolute:
            return absolute
    return None


def nearest_relevant_container(node: Tag) -> Tag:
    current: Tag | None = node
    while current is not None:
        if current.name in {"article", "li", "tr"}:
            return current
        classes = " ".join(current.get("class", []))
        if any(t in classes.lower() for t in ("item", "photo", "post", "gallery", "thumb")):
            return current
        parent = current.parent
        current = parent if isinstance(parent, Tag) else None
    return node


def find_plate_in_container(container: Tag) -> str | None:
    texts: list[str] = []
    for tag in container.find_all(True):
        for attr in ("alt", "title", "aria-label"):
            v = tag.get(attr)
            if v:
                texts.append(v)
    texts.extend(container.stripped_strings)
    for text in texts:
        for candidate in extract_plate_candidates(text):
            return candidate
    return None


def derive_filename(image_url: str, plate_text: str) -> str:
    """plate_text + last 6 alphanum chars of original stem to avoid collisions.
    Example: 01A123BA_f3c8a1.jpg
    """
    suffix    = Path(image_url.split("?", 1)[0]).suffix.lower() or ".jpg"
    stem_tail = re.sub(r"[^0-9A-Za-z]", "", Path(image_url.split("?", 1)[0]).stem)[-6:]
    return f"{plate_text}_{stem_tail}{suffix}"


def parse_gallery_records(
    html: str, page_url: str, segment: Segment, page_number: int
) -> list[PlateRecord]:
    soup = BeautifulSoup(html, "lxml")
    records: list[PlateRecord] = []
    seen_urls: set[str] = set()

    for node in soup.find_all(["img", "a"]):
        image_url = extract_image_url_from_tag(node, page_url)
        if not image_url or image_url in seen_urls:
            continue
        container     = nearest_relevant_container(node)
        plate_display = find_plate_in_container(container)
        if not plate_display:
            continue
        plate_text = normalize_plate_text(plate_display)
        if not looks_like_plate(plate_text):
            continue

        records.append(PlateRecord(
            filename=derive_filename(image_url, plate_text),
            image_url=image_url,
            plate_text=plate_text,
            plate_display=normalize_plate_display(plate_display),
            source_page=page_number,
            source_url=page_url,
            region_name=segment.region_name,
            region_title=segment.region_title,
            region_id=segment.region_id,
            mask=segment.mask,
            ctype=segment.ctype,
            ctype_label=CTYPE_LABELS.get(segment.ctype, str(segment.ctype)),
        ))
        seen_urls.add(image_url)

    return records


# ---------------------------------------------------------------------------
# Segment scanner
# ---------------------------------------------------------------------------

async def scan_segment(
    scanner: AsyncScanner,
    segment: Segment,
    *,
    start_page: int,
    max_pages: int,
    debug_dir: Path,
    save_debug_on_error: bool,
) -> list[PlateRecord]:
    LOGGER.info("▶ %s", segment.label)
    records: list[PlateRecord] = []
    prev_sig: tuple[str, ...] | None = None

    for page in range(start_page, max_pages + 1):
        url          = segment.page_url(page)
        html         = await scanner.fetch_gallery(url)
        page_records = parse_gallery_records(html, url, segment, page)

        if save_debug_on_error and not page_records and page == start_page:
            save_debug_html(debug_dir, f"empty_{segment.region_name}_ctype{segment.ctype}", html)

        if not page_records:
            LOGGER.info("■ %s stopped page=%s (empty)", segment.label, page)
            break

        sig = tuple(r.image_url for r in page_records[:8])
        if prev_sig and sig == prev_sig:
            LOGGER.info("■ %s stopped page=%s (repeating)", segment.label, page)
            break

        prev_sig = sig
        records.extend(page_records)
        LOGGER.info("✔ %s page=%s +%s total=%s", segment.label, page, len(page_records), len(records))

    return records


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

async def run_scan(args: argparse.Namespace, cookies: dict[str, str]) -> "pd.DataFrame":
    import pandas as pd

    region_filter = set(args.regions.split(",")) if args.regions else None
    ctypes        = [int(x) for x in args.ctypes.split(",")]
    segments      = build_segments(region_filter=region_filter, ctypes=ctypes)

    if not segments:
        raise ValueError("No segments selected. Check --regions / --ctypes.")

    semaphore   = asyncio.Semaphore(args.segment_concurrency)
    all_records: list[PlateRecord] = []

    async with AsyncScanner(
        timeout_seconds=args.timeout,
        rotate_every=args.rotate_every,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        cookie_reset_every=args.cookie_reset_every,
        cookies=cookies,
        debug_dir=Path(args.debug_dir),
        save_debug_on_error=args.save_debug_html,
        max_retries=args.max_retries,
    ) as scanner:

        async def bounded(seg: Segment) -> list[PlateRecord]:
            async with semaphore:
                return await scan_segment(
                    scanner, seg,
                    start_page=args.start_page,
                    max_pages=args.max_pages,
                    debug_dir=Path(args.debug_dir),
                    save_debug_on_error=args.save_debug_html,
                )

        results = await asyncio.gather(*(bounded(s) for s in segments))

    for records in results:
        all_records.extend(records)

    if not all_records:
        raise RuntimeError(
            "No records parsed.\n"
            "  → Run with --use-browser to solve captcha interactively."
        )

    frame = pd.DataFrame(asdict(r) for r in all_records)
    frame = frame.drop_duplicates(subset=["image_url"]).sort_values(
        by=["region_id", "ctype", "source_page", "filename"]
    )
    return frame


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Async PlatesMania scanner — Uzbek LPRNet dataset builder."
    )
    p.add_argument("--output-csv",          default=str(DEFAULT_OUTPUT))
    p.add_argument("--regions",             default="",
                   help="Comma-separated region keys. Empty = all regions.")
    p.add_argument("--ctypes",              default="1,2")
    p.add_argument("--start-page",          type=int,   default=1)
    p.add_argument("--max-pages",           type=int,   default=DEFAULT_MAX_PAGES)
    p.add_argument("--segment-concurrency", type=int,   default=DEFAULT_SEGMENT_CONCURRENCY,
                   help="Parallel segments. Keep <= 3 to stay safe.")
    p.add_argument("--delay-min",           type=float, default=DEFAULT_DELAY_MIN)
    p.add_argument("--delay-max",           type=float, default=DEFAULT_DELAY_MAX)
    p.add_argument("--rotate-every",        type=int,   default=DEFAULT_HEADERS_ROTATE_EVERY)
    p.add_argument("--cookie-reset-every",  type=int,   default=300)
    p.add_argument("--timeout",             type=float, default=45.0)
    p.add_argument("--max-retries",         type=int,   default=4)
    # Cookie sources (mutually exclusive, priority: browser > file > header)
    p.add_argument("--use-browser",   action="store_true",
                   help="Open Chrome so you can solve captcha, then scan with harvested cookies.")
    p.add_argument("--driver-path",   default="",
                   help="Path to chromedriver.exe (optional, only with --use-browser).")
    p.add_argument("--cookies-file",  default="",
                   help="JSON cookie export or plain Cookie header text file.")
    p.add_argument("--cookie-header", default="",
                   help="Raw Cookie header string, e.g. 'a=1; b=2'.")
    p.add_argument("--debug-dir",         default=str(DEFAULT_DEBUG_DIR))
    p.add_argument("--save-debug-html",   action="store_true")
    p.add_argument("--log-level",         default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def resolve_cookies(args: argparse.Namespace) -> dict[str, str]:
    """
    Priority order:
      1. --use-browser  → open Chrome, solve captcha, harvest cookies
         (if dataset/session_cookies.json already exists, reuse it without opening browser)
      2. --cookies-file / --cookie-header  → load from file or string
      3. nothing        → empty dict (will fail if site requires verification)
    """
    if args.use_browser:
        if SAVED_COOKIES_PATH.exists():
            LOGGER.info("Reusing saved cookies from %s", SAVED_COOKIES_PATH)
            try:
                return _json_to_dict(json.loads(SAVED_COOKIES_PATH.read_text(encoding="utf-8")))
            except Exception as exc:
                LOGGER.warning("Could not load saved cookies (%s), opening browser.", exc)
        return harvest_cookies_via_browser(args.driver_path or None)

    return parse_cookie_input(
        cookie_header=args.cookie_header or None,
        cookies_file=Path(args.cookies_file) if args.cookies_file else None,
    )


def main() -> int:
    parser = build_parser()
    args   = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    if args.delay_min > args.delay_max:
        parser.error("--delay-min cannot exceed --delay-max")
    if args.start_page < 1:
        parser.error("--start-page must be >= 1")
    if args.max_pages < args.start_page:
        parser.error("--max-pages must be >= --start-page")

    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    (output_path.parent / "images").mkdir(parents=True, exist_ok=True)

    cookies = resolve_cookies(args)

    try:
        frame = asyncio.run(run_scan(args, cookies))
    except AntiBotError as exc:
        LOGGER.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted by user")
        return 130

    frame.to_csv(output_path, index=False, encoding="utf-8")
    LOGGER.info("Saved %s unique links → %s", len(frame), output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
