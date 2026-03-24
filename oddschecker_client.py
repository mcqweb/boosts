"""
OddsChecker web-scraping client.

Provides helpers to:
  - Convert Betfair match IDs to OddsChecker page slugs
  - Scrape market IDs (FGS / AGS) from the OddsChecker match page
  - Fetch odds via the /api/markets/v2/all-odds endpoint using tls_client

Original implementation supplied by the project author; integrated here with
minor structural adjustments (logging_config import, proxy helper, etc.).
"""

import json
import os
import re
import time
import traceback

import requests

try:
    import cloudscraper
except ImportError:
    cloudscraper = None

try:
    import tls_client
except ImportError:
    tls_client = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

from config import ODDSMATCHA_API_KEY
from logging_config import get_logger

logger = get_logger("oddschecker_client")

# ---------------------------------------------------------------------------
# Module-level proxy config — call set_proxies() from your entry point.
# ---------------------------------------------------------------------------
_OC_PROXIES = None


def set_proxies(proxies) -> None:
    """Set the requests proxy dict used for all OddsChecker API calls."""
    global _OC_PROXIES
    _OC_PROXIES = proxies


# ---------------------------------------------------------------------------
DEBUG_MODE = os.getenv("DEBUG_MODE", "0") == "1"


def _debug(msg: str) -> None:
    if DEBUG_MODE:
        print(msg, flush=True)


def _session_get(session, url, **kwargs):
    """Perform session.get with a compatible timeout parameter."""
    if tls_client and isinstance(session, tls_client.sessions.Session):
        kwargs["timeout_seconds"] = kwargs.get("timeout_seconds", REQUEST_TIMEOUT)
    else:
        kwargs["timeout"] = kwargs.get("timeout", REQUEST_TIMEOUT)
    return session.get(url, **kwargs)


def _make_session():
    """
    Return a session for OddsChecker web API access.

    Preferred order:
      1) cloudscraper
      2) tls_client
      3) requests
    """
    if cloudscraper:
        try:
            return cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
        except Exception as e:
            logger.warning("Could not create cloudscraper session: %s", e)

    if tls_client:
        try:
            return tls_client.Session(
                client_identifier="chrome120",
                random_tls_extension_order=True,
            )
        except Exception as e:
            logger.warning("Could not create tls_client session: %s", e)

    logger.warning("Using requests.Session fallback (may be blocked)")
    return requests.Session()


# ---------------------------------------------------------------------------
# Fuzzy name matching
# ---------------------------------------------------------------------------

def _fuzzy_match_names(name1: str, name2: str) -> bool:
    """
    Return True if two player name strings are close enough to be the same
    person.  Requires ≥2 matching word-parts, or >50 % match rate for short
    names.
    """
    parts1 = {p for p in name1.lower().split() if len(p) > 1}
    parts2 = {p for p in name2.lower().split() if len(p) > 1}

    if not parts1 or not parts2:
        return False

    matches = len(parts1 & parts2)
    total = len(parts1 | parts2)

    if total == 0:
        return False

    return matches >= 2 or (matches / total) >= 0.5


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

CACHE_DIR = "./cache"
CACHE_HTML_SUBDIR = "html"
CACHE_ODDS_SUBDIR = "odds"
ODDS_CACHE_TTL = 60  # seconds
REQUEST_TIMEOUT = 30

BOOKMAKER_MAPPING = {
    "B3":  "Bet365",
    "FR":  "Betfred",
    "PP":  "Paddy Power",
    "WH":  "William Hill",
    "BF":  "Betfair",
    "SK":  "Sky Bet",
    "LD":  "Ladbrokes",
    "UN":  "Unibet",
    "888": "888sport",
    "BX":  "Betdaq",
    "MR":  "Matchbook",
    "SM":  "Smarkets",
    "VE":  "VirginBet",
    "VC":  "Bet Victor",
    "SX":  "Spreadex",
    "CE":  "Coral",
    "CR":  "Coral",
    "WA":  "Betway",
    "BW":  "Betway",
    "BY":  "Boylesports",
    "KN":  "BetMGM UK",
    "OE":  "10Bet",
    "10B": "10Bet",
    "QN":  "QuinnBet",
    "SI":  "Sporting Index",
    "EE":  "888 Sport",
    "AKB": "AKBets",
    "BRS": "Bresbet",
    "PUP": "Priced Up",
    "S6":  "Star Sports",
    "BTT": "BetTom",
    "G5":  "Bet Goodwin",
    "CUS": "Casumo",
}

_SLUG_CACHE: dict[str, str] = {}


def _ensure_cache_dirs() -> None:
    os.makedirs(os.path.join(CACHE_DIR, CACHE_HTML_SUBDIR), exist_ok=True)
    os.makedirs(os.path.join(CACHE_DIR, CACHE_ODDS_SUBDIR), exist_ok=True)


def _get_bookmaker_name(code: str) -> str:
    return BOOKMAKER_MAPPING.get(code, code)


def _get_cache_path(filename: str, subdir: str) -> str:
    safe = filename
    for ch in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '&', '=', ' '] :
        safe = safe.replace(ch, '_')
    if len(safe) > 180:
        import hashlib
        h = hashlib.sha256(safe.encode('utf-8')).hexdigest()
        safe = safe[:100] + '_' + h
    return os.path.join(CACHE_DIR, subdir, safe)


def _cache_valid(cache_path: str, max_age=None) -> bool:
    if not os.path.exists(cache_path):
        return False
    if max_age is None:
        return True
    return (time.time() - os.path.getmtime(cache_path)) < max_age


def _read_cache(cache_path: str):
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Failed to read cache %s: %s", cache_path, e)
        return None


def _write_cache(cache_path: str, data) -> None:
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        _debug(f"[INFO] Cached data to {cache_path}")
    except Exception as e:
        logger.warning("Failed to write cache %s: %s", cache_path, e)


def _write_cache_text(cache_path: str, data: str) -> None:
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(data)
        _debug(f"[INFO] Cached HTML to {cache_path}")
    except Exception as e:
        logger.warning("Failed to write text cache %s: %s", cache_path, e)


def _read_cache_text(cache_path: str):
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logger.warning("Failed to read text cache %s: %s", cache_path, e)
        return None


# ---------------------------------------------------------------------------
# Slug conversion
# ---------------------------------------------------------------------------

def prefetch_oddschecker_slugs(betfair_ids: list) -> dict:
    """
    Batch-convert Betfair match IDs → OddsChecker page slugs.
    Results are stored in the module-level _SLUG_CACHE.
    """
    if not betfair_ids:
        return {}

    uncached = [str(bid) for bid in betfair_ids if str(bid) not in _SLUG_CACHE]
    if not uncached:
        return _SLUG_CACHE

    ids_str = ",".join(uncached)
    url = f"https://api.oddsmatcha.uk/convert/betfair_to_oddschecker?betfair_ids={ids_str}"

    try:
        _debug(f"[INFO] Prefetching slugs for {len(uncached)} Betfair IDs…")
        headers = {}
        if ODDSMATCHA_API_KEY:
            headers["X-API-Key"] = ODDSMATCHA_API_KEY
        else:
            logger.warning("ODDSMATCHA_API_KEY is not set; oddsmatcha slug conversion may be rejected")

        r = requests.get(url, proxies=_OC_PROXIES, timeout=30, headers=headers or None)
        print(f"[OC] Slug API status: {r.status_code}")
        if r.status_code != 200:
            logger.warning("Slug API returned %s", r.status_code)
            return _SLUG_CACHE

        data = r.json()
        if data.get("success") and isinstance(data.get("conversions"), list):
            ok = 0
            for conv in data["conversions"]:
                bid = str(conv.get("betfair_id", ""))
                slug = conv.get("page_slug")
                if bid and slug:
                    _SLUG_CACHE[bid] = slug
                    ok += 1
            print(f"[OC] Prefetched {ok}/{len(uncached)} slugs")
        else:
            logger.warning("Unexpected slug API response: %s", str(data)[:400])

    except json.JSONDecodeError as e:
        logger.error("JSON error from slug API: %s", e)
    except Exception as e:
        logger.exception("Slug prefetch error: %s", e)

    return _SLUG_CACHE


def get_oddschecker_match_slug(betfair_id) -> str | None:
    """Convert a single Betfair match ID to an OddsChecker page slug."""
    if isinstance(betfair_id, dict):
        betfair_id = next(iter(betfair_id.values()))

    if betfair_id in _SLUG_CACHE:
        return _SLUG_CACHE[betfair_id]

    url = f"https://api.oddsmatcha.uk/convert/betfair_to_oddschecker?betfair_ids={betfair_id}"
    try:
        headers = {}
        if ODDSMATCHA_API_KEY:
            headers["X-API-Key"] = ODDSMATCHA_API_KEY
        else:
            logger.warning("ODDSMATCHA_API_KEY is not set; oddsmatcha slug conversion may be rejected")

        r = requests.get(url, proxies=_OC_PROXIES, timeout=10, headers=headers or None)
        data = r.json()
        if data.get("success") and isinstance(data.get("conversions"), list):
            convs = data["conversions"]
            if convs:
                slug = convs[0].get("page_slug")
                if slug:
                    _SLUG_CACHE[betfair_id] = slug
                    return slug
    except Exception as e:
        logger.exception("slug lookup failed for %s: %s", betfair_id, e)

    return None


# ---------------------------------------------------------------------------
# Market-page scraping
# ---------------------------------------------------------------------------

def scrape_oddschecker_market_ids(match_slug: str):
    """
    Scrape the OddsChecker match page to find FGS / AGS market IDs.

    Returns:
        (market_ids_dict, tls_session) or (None, None) on failure.
        market_ids_dict has keys 'fgs' and/or 'ags'.
    """
    _ensure_cache_dirs()

    if not BeautifulSoup:
        logger.error("BeautifulSoup4 not installed — cannot scrape page")
        return None, None

    url = f"https://www.oddschecker.com/football/{match_slug}/winner"
    cache_filename = f"{match_slug.replace('/', '_')}_page.html"
    cache_path = _get_cache_path(cache_filename, CACHE_HTML_SUBDIR)

    html_content = None
    session = _make_session()

    try:
        if _cache_valid(cache_path, max_age=None):
            _debug(f"[INFO] Using cached HTML for {match_slug}")
            html_content = _read_cache_text(cache_path)
        else:
            headers = {
                "authority": "www.oddschecker.com",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "en-GB,en;q=0.9",
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            }
            cookies = {
                "odds_type": "decimal",
                "device": "desktop",
                "logged_in": "false",
                "mobile_redirect": "true",
            }

            _debug(f"[INFO] Fetching {url}")
            resp = _session_get(session, url, headers=headers, cookies=cookies)
            if resp.status_code == 404:
                logger.info("No OddsChecker page at %s (404)", url)
                return None, None
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code} fetching {url}")

            html_content = resp.text
            _write_cache_text(cache_path, html_content)

        # Debug dump
        try:
            with open("oddschecker_market_page.html", "w", encoding="utf-8") as f:
                f.write(html_content)
        except Exception:
            pass

        # Parse market IDs from inline script JSON
        soup = BeautifulSoup(html_content, "html.parser")
        market_ids = {"fgs": None, "ags": None}

        for script in soup.find_all("script"):
            content = script.get_text() or ""
            if "marketName" not in content:
                continue

            fgs = re.findall(
                r'"ocMarketId":(\d+),[^}]*"marketName":"[^#]*#First Goalscorer"',
                content,
            )
            if fgs:
                market_ids["fgs"] = fgs[0]

            ags = re.findall(
                r'"ocMarketId":(\d+),[^}]*"marketName":"[^#]*#Anytime Goalscorer"',
                content,
            )
            if ags:
                market_ids["ags"] = ags[0]

        if market_ids["fgs"] or market_ids["ags"]:
            _debug(f"[INFO] Market IDs: FGS={market_ids['fgs']}, AGS={market_ids['ags']}")
            return market_ids, session

        logger.warning("Could not find market IDs for %s", match_slug)
        return None, None

    except Exception as e:
        logger.exception("Error scraping OddsChecker page for %s: %s", match_slug, e)
        return None, None


# ---------------------------------------------------------------------------
# Odds fetching
# ---------------------------------------------------------------------------

def get_oddschecker_odds_web_fallback(market_ids: list, session=None):
    """
    Fetch /api/markets/v2/all-odds for the given list of OddsChecker market IDs.
    Results are cached for ODDS_CACHE_TTL seconds.
    """
    _ensure_cache_dirs()

    cache_filename = f"odds_{'_'.join(str(m) for m in market_ids)}.json"
    cache_path = _get_cache_path(cache_filename, CACHE_ODDS_SUBDIR)

    if _cache_valid(cache_path, max_age=ODDS_CACHE_TTL):
        _debug(f"[INFO] Using cached odds for {market_ids}")
        return _read_cache(cache_path)

    if session is None:
        session = _make_session()

    ids_str = ",".join(str(m) for m in market_ids)
    url = f"https://www.oddschecker.com/api/markets/v2/all-odds?market-ids={ids_str}&repub=OC"

    headers = {
        "authority": "www.oddschecker.com",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-GB,en;q=0.9",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }
    cookies = {
        "odds_type": "decimal",
        "device": "desktop",
        "logged_in": "false",
        "mobile_redirect": "true",
    }

    try:
        _debug(f"[INFO] Fetching odds from {url}")
        resp = _session_get(session, url, headers=headers, cookies=cookies)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}")
        data = resp.json()
        _write_cache(cache_path, data)

        try:
            with open("whale_oc_web_fallback.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        return data
    except Exception as e:
        logger.exception("Failed to fetch odds for %s: %s", market_ids, e)
        return None


# ---------------------------------------------------------------------------
# High-level: get odds + arb opportunities for a match
# ---------------------------------------------------------------------------

def get_oddschecker_odds(match_slug: str, betdata: list | dict) -> list:
    """
    Scrape FGS/AGS odds for *match_slug* and return arbitrage opportunities
    where OddsChecker odds > lay_odds supplied in *betdata*.
    """
    market_ids, session = scrape_oddschecker_market_ids(match_slug)
    if not market_ids:
        logger.error("No market IDs for %s", match_slug)
        return []

    player_map = _extract_player_bets_from_html("oddschecker_market_page.html", market_ids)

    id_list = [v for k, v in market_ids.items() if v]
    if not id_list:
        logger.error("Empty market ID list for %s", match_slug)
        return []

    oc_data = get_oddschecker_odds_web_fallback(id_list, session=session)
    if oc_data is None:
        logger.error("No odds data for %s", match_slug)
        return []

    try:
        with open("whale_oc.json", "w", encoding="utf-8") as f:
            json.dump(oc_data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    api_map = _extract_player_bets_from_api(oc_data, market_ids)
    for bettype, players in api_map.items():
        player_map.setdefault(bettype, {}).update(players)

    if not isinstance(betdata, list):
        betdata = [betdata]

    arbs = []
    for bet in betdata:
        bettype = bet.get("bettype")
        outcome = bet.get("outcome")
        min_odds = float(bet.get("min_odds", 0))
        lay_odds = float(bet.get("lay_odds", 0))

        # Resolve betId
        bet_id = None
        for name, bid in player_map.get(bettype, {}).items():
            if name.lower() == outcome.lower():
                bet_id = bid
                break
        if not bet_id:
            for name, bid in player_map.get(bettype, {}).items():
                if _fuzzy_match_names(name, outcome):
                    bet_id = bid
                    logger.info("Fuzzy matched '%s' → '%s'", outcome, name)
                    break

        if not bet_id:
            logger.warning("No betId for %s / %s", bettype, outcome)
            continue

        for market in oc_data:
            if bettype not in market.get("marketName", ""):
                continue
            for entry in market.get("odds", []):
                if entry.get("betId") != bet_id:
                    continue
                try:
                    odds = float(entry.get("oddsDecimal", 0))
                    bookie = _get_bookmaker_name(entry.get("bookmakerCode", ""))
                    if odds > 0 and lay_odds > 0 and odds > lay_odds:
                        arbs.append({
                            "bettype": bettype,
                            "outcome": outcome,
                            "odds": odds,
                            "bookie": bookie,
                            "lay_odds": lay_odds,
                        })
                except (ValueError, TypeError):
                    continue

    logger.debug("get_oddschecker_odds(%s) → %d arbs", match_slug, len(arbs))
    return arbs


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_player_bets_from_html(html_file: str, market_ids: dict) -> dict:
    player_bets: dict[str, dict] = {}
    try:
        with open(html_file, "r", encoding="utf-8") as f:
            html = f.read()

        pat = r'<script[^>]*data-hypernova-key="subeventmarkets"[^>]*><!--({.*?})--></script>'
        m = re.search(pat, html, re.DOTALL)
        if not m:
            return {}

        data = json.loads(m.group(1))
        entities = data.get("bestOdds", {}).get("bets", {}).get("entities", {})

        fgs_id = int(market_ids["fgs"]) if market_ids.get("fgs") else None
        ags_id = int(market_ids["ags"]) if market_ids.get("ags") else None

        for bet_id_str, bet_data in entities.items():
            name = bet_data.get("betName", "")
            mid = bet_data.get("marketId")
            if not name or mid is None:
                continue
            if fgs_id and mid == fgs_id:
                player_bets.setdefault("First Goalscorer", {})[name] = int(bet_id_str)
            elif ags_id and mid == ags_id:
                player_bets.setdefault("Anytime Goalscorer", {})[name] = int(bet_id_str)

    except Exception as e:
        logger.warning("HTML extraction error: %s", e)

    return player_bets


def _extract_player_bets_from_api(oc_data: list, market_ids: dict) -> dict:
    player_bets: dict[str, dict] = {}

    fgs_id = int(market_ids["fgs"]) if market_ids.get("fgs") else None
    ags_id = int(market_ids["ags"]) if market_ids.get("ags") else None

    try:
        for market in oc_data:
            mid = int(market.get("marketId", 0))
            if fgs_id and mid == fgs_id:
                bettype = "First Goalscorer"
            elif ags_id and mid == ags_id:
                bettype = "Anytime Goalscorer"
            else:
                continue

            for bet in market.get("bets", []):
                bid = bet.get("betId")
                name = bet.get("betName")
                if bid and name:
                    player_bets.setdefault(bettype, {})[name] = bid

    except Exception as e:
        logger.warning("API extraction error: %s", e)

    return player_bets
