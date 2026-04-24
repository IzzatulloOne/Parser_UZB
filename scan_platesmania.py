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


class RateLimitError(RuntimeError):
    """HTTP 429 / 503 received."""


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
            # Sec-* headers only make sense for Chrome UA
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
                if chrome
                else {}
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
        self.timeout_seconds    = timeout_seconds
        self.delay_min          = delay_min
        self.delay_max          = delay_max
        self.cookie_reset_every = max(0, cookie_reset_every)
        self.debug_dir          = debug_dir
        self.save_debug_on_error = save_debug_on_error
        self.max_retries        = max_retries
        self.rotator            = HeaderRotator(rotate_every=rotate_every)
        self._client: httpx.AsyncClient | None = None
        self._cookies           = cookies.copy()
        self._lock              = asyncio.Lock()
        self._page_fetch_count  = 0

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
            # Reuse connections — faster AND looks more like a browser
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            http2=True,   # HTTP/2 keeps fewer connections open → less suspicious
        )

    async def _warmup(self) -> None:
        """Visit the main page first so the session looks human."""
        if self._client is None:
            return
        try:
            await self._client.get(f"{BASE_URL}/uz/")
            await asyncio.sleep(random.uniform(1.5, 3.0))
            LOGGER.debug("Warmup request done")
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Warmup failed (non-fatal): %s", exc)

    async def _refresh_headers_and_cookies(self) -> None:
        async with self._lock:
            if self._client is None:
                self._client = self._make_client()
                return
            self._page_fetch_count += 1
            self._client.headers.update(self.rotator.next_headers())
            if (
                self.cookie_reset_every
                and self._page_fetch_count % self.cookie_reset_every == 0
            ):
                LOGGER.info("Resetting cookies after %s page requests", self._page_fetch_count)
                self._client.cookies.clear()
                self._client.cookies.update(self._cookies)

    async def fetch_gallery(self, url: str) -> str:
        """Fetch one gallery page with retry logic for rate limiting."""
        for attempt in range(self.max_retries):
            await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))
            await self._refresh_headers_and_cookies()

            if self._client is None:
                raise RuntimeError("HTTP client is not initialized")

            try:
                response = await self._client.get(url)
            except httpx.TimeoutException:
                wait = (2 ** attempt) * random.uniform(3.0, 6.0)
                LOGGER.warning("Timeout on %s, retry %s/%s, waiting %.1fs", url, attempt + 1, self.max_retries, wait)
                await asyncio.sleep(wait)
                continue
            except httpx.RequestError as exc:
                LOGGER.warning("Request error: %s — retry %s/%s", exc, attempt + 1, self.max_retries)
                await asyncio.sleep(random.uniform(5.0, 10.0))
                continue

            # Rate limiting — back off exponentially
            if response.status_code in (429, 503):
                wait = (2 ** attempt) * random.uniform(15.0, 30.0)
                LOGGER.warning(
                    "HTTP %s on %s, backing off %.1fs (attempt %s/%s)",
                    response.status_code, url, wait, attempt + 1, self.max_retries,
                )
                await asyncio.sleep(wait)
                continue

            response.raise_for_status()
            html = response.text

            if is_antibot_page(html):
                if self.save_debug_on_error:
                    save_debug_html(self.debug_dir, "killbot_block", html)
                raise AntiBotError(
                    "PlatesMania returned KillBot verification. "
                    "Pass valid cookies via --cookies-file or --cookie-header."
                )

            return html

        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} retries")


# ---------------------------------------------------------------------------
# Anti-bot detection
# ---------------------------------------------------------------------------

def is_antibot_page(html: str) -> bool:
    lowered = html.lower()
    markers = (
        "killbot user verification",
        "user verification",
        "window.kberrors",
        "id='kb-recaptcha'",
        'id="kb-recaptcha"',
    )
    return any(m in lowered for m in markers)


def save_debug_html(debug_dir: Path, prefix: str, html: str) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    path = debug_dir / f"{prefix}.html"
    path.write_text(html, encoding="utf-8")
    LOGGER.info("Saved debug HTML → %s", path)


# ---------------------------------------------------------------------------
# Segment builder
# ---------------------------------------------------------------------------

def build_segments(region_filter: set[str] | None, ctypes: list[int]) -> list[Segment]:
    segments: list[Segment] = []
    for item in REGION_CONFIG:
        if region_filter and item["name"] not in region_filter:
            continue
        for ctype in ctypes:
            segments.append(
                Segment(
                    region_name=item["name"],
                    region_title=item["title"],
                    region_id=item["r_id"],
                    mask=item["mask"],
                    ctype=ctype,
                )
            )
    return segments


# ---------------------------------------------------------------------------
# Cookie helpers
# ---------------------------------------------------------------------------

def parse_cookie_input(cookie_header: str | None, cookies_file: Path | None) -> dict[str, str]:
    if cookie_header:
        return cookie_header_to_dict(cookie_header)
    if not cookies_file:
        return {}
    raw = cookies_file.read_text(encoding="utf-8").strip()
    if not raw:
        return {}
    if raw.lstrip().startswith(("{", "[")):
        return parse_cookie_json(json.loads(raw))
    return cookie_header_to_dict(raw)


def parse_cookie_json(payload: object) -> dict[str, str]:
    if isinstance(payload, dict):
        if "cookies" in payload and isinstance(payload["cookies"], list):
            return {str(i["name"]): str(i["value"]) for i in payload["cookies"] if "name" in i}
        return {str(k): str(v) for k, v in payload.items()}
    if isinstance(payload, list):
        return {str(i["name"]): str(i["value"]) for i in payload if "name" in i}
    raise ValueError("Unsupported cookie JSON format")


def cookie_header_to_dict(value: str) -> dict[str, str]:
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
# Plate text normalization
# ---------------------------------------------------------------------------

def normalize_plate_display(text: str) -> str:
    text = text.replace("\xa0", " ").strip()
    text = re.sub(r"^image\s*:\s*", "", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).upper()


def normalize_plate_text(text: str) -> str:
    normalized = normalize_plate_display(text)
    normalized = re.sub(r"[^0-9A-ZА-ЯІЇЄҐ ]+", "", normalized)
    return normalized.replace(" ", "")


def looks_like_plate(text: str) -> bool:
    compact = normalize_plate_text(text)
    digits  = sum(c.isdigit() for c in compact)
    letters = sum(c.isalpha() for c in compact)
    return 6 <= len(compact) <= 10 and digits >= 2 and letters >= 1


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
    """Filename = plate number + original extension.

    To avoid collisions when the same plate is photographed multiple times,
    we append the last 6 characters of the original image stem.
    Result example: 01A123BA_f3c8a1.jpg
    """
    suffix = Path(image_url.split("?", 1)[0]).suffix.lower() or ".jpg"
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
        container    = nearest_relevant_container(node)
        plate_display = find_plate_in_container(container)
        if not plate_display:
            continue
        plate_text = normalize_plate_text(plate_display)
        if not looks_like_plate(plate_text):
            continue

        records.append(
            PlateRecord(
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
            )
        )
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
    LOGGER.info("▶ Scanning %s", segment.label)
    records: list[PlateRecord] = []
    previous_signature: tuple[str, ...] | None = None

    for page in range(start_page, max_pages + 1):
        url          = segment.page_url(page)
        html         = await scanner.fetch_gallery(url)
        page_records = parse_gallery_records(html, url, segment, page)

        if save_debug_on_error and not page_records and page == start_page:
            save_debug_html(debug_dir, f"empty_{segment.region_name}_ctype{segment.ctype}", html)

        if not page_records:
            LOGGER.info("■ %s stopped at page=%s (no rows)", segment.label, page)
            break

        sig = tuple(r.image_url for r in page_records[:8])
        if previous_signature and sig == previous_signature:
            LOGGER.info("■ %s stopped at page=%s (content repeating)", segment.label, page)
            break

        previous_signature = sig
        records.extend(page_records)
        LOGGER.info("✔ %s page=%s parsed=%s total=%s", segment.label, page, len(page_records), len(records))

    return records


# ---------------------------------------------------------------------------
# Main scan runner
# ---------------------------------------------------------------------------

async def run_scan(args: argparse.Namespace) -> "pd.DataFrame":
    import pandas as pd

    region_filter = set(args.regions.split(",")) if args.regions else None
    ctypes        = [int(x) for x in args.ctypes.split(",")]
    cookies       = parse_cookie_input(
        cookie_header=args.cookie_header or None,
        cookies_file=Path(args.cookies_file) if args.cookies_file else None,
    )
    segments = build_segments(region_filter=region_filter, ctypes=ctypes)

    if not segments:
        raise ValueError("No segments selected. Check --regions / --ctypes.")

    semaphore    = asyncio.Semaphore(args.segment_concurrency)
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

        async def bounded_scan(segment: Segment) -> list[PlateRecord]:
            async with semaphore:
                return await scan_segment(
                    scanner,
                    segment,
                    start_page=args.start_page,
                    max_pages=args.max_pages,
                    debug_dir=Path(args.debug_dir),
                    save_debug_on_error=args.save_debug_html,
                )

        results = await asyncio.gather(*(bounded_scan(s) for s in segments))

    for records in results:
        all_records.extend(records)

    if not all_records:
        raise RuntimeError(
            "No records parsed. If PlatesMania returned KillBot, supply cookies "
            "from a verified browser session via --cookies-file or --cookie-header."
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
                   help="Concurrent segments (keep ≤ 3 to stay safe).")
    p.add_argument("--delay-min",           type=float, default=DEFAULT_DELAY_MIN)
    p.add_argument("--delay-max",           type=float, default=DEFAULT_DELAY_MAX)
    p.add_argument("--rotate-every",        type=int,   default=DEFAULT_HEADERS_ROTATE_EVERY)
    p.add_argument("--cookie-reset-every",  type=int,   default=300)
    p.add_argument("--timeout",             type=float, default=45.0)
    p.add_argument("--max-retries",         type=int,   default=4)
    p.add_argument("--cookies-file",        default="")
    p.add_argument("--cookie-header",       default="")
    p.add_argument("--debug-dir",           default=str(DEFAULT_DEBUG_DIR))
    p.add_argument("--save-debug-html",     action="store_true")
    p.add_argument("--log-level",           default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


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

    try:
        frame = asyncio.run(run_scan(args))
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