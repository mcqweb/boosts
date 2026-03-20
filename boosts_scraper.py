"""
boosts_scraper.py
=================
Fetches "betting boosts" (enhanced / boosted prices) from the OddsChecker
mobile API.

Key public API
--------------
  get_boosts(...)          - fetch raw boost bets from /v1/bets-search
  get_subevents_hierarchy(...) - fetch event tree from /v1/subevents-hierarchy
  get_categories()         - fetch all sport categories
  get_bookmakers()         - fetch all bookmaker definitions
  get_big_football_matches() - upcoming high-profile football matches
  get_horse_racing_next_off() - next horse-racing events
  format_boosts(boosts)    - pretty-print a list of boost dicts
  run_boost_loop(...)      - continually poll and display new boosts

Authentication
--------------
The OddsChecker mobile API lives at api.oddschecker.com and uses TLS
fingerprinting (same Cloudflare protection as the main site).  We use
tls_client with a Chrome fingerprint, exactly as the web-scraping code does.

If an OC_API_KEY environment variable is set it will be forwarded as an
``X-Api-Key`` header (optional — most endpoints work without it when the
correct TLS fingerprint is presented).
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

try:
    import cloudscraper
except ImportError:
    cloudscraper = None  # type: ignore

try:
    import tls_client
except ImportError:
    tls_client = None  # type: ignore

import requests

from config import (
    ALL_BOOST_BET_TYPE_IDS,
    BOOKMAKER_MAPPING,
    BOOST_BET_TYPE_IDS_FOOTBALL,
    CACHE_DIR,
    CACHE_TTL_BOOSTS,
    CATEGORY_GROUP_FOOTBALL,
    DEFAULT_BOOKMAKER_CODES,
    DEFAULT_MINIMUM_ODDS,
    DEFAULT_PAGE_SIZE,
    OC_API_BASE,
    OC_API_KEY,
    REQUEST_TIMEOUT,
)
from logging_config import get_logger

logger = get_logger("boosts_scraper")

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

_BOOSTS_CACHE_DIR = os.path.join(CACHE_DIR, "boosts")


def _ensure_cache() -> None:
    os.makedirs(_BOOSTS_CACHE_DIR, exist_ok=True)


def _cache_path(key: str) -> str:
    # Remove characters that are invalid in Windows filenames and reduce length
    safe = key.replace("/", "_")
    safe = safe.replace("\\", "_")
    safe = safe.replace(":", "_")
    safe = safe.replace("*", "_")
    safe = safe.replace("?", "_")
    safe = safe.replace('"', "_")
    safe = safe.replace("<", "_")
    safe = safe.replace(">", "_")
    safe = safe.replace("|", "_")
    safe = safe.replace("&", "_")
    safe = safe.replace("=", "_")
    safe = safe.replace(" ", "_")

    # Prevent too-long names; use hash suffix if needed
    if len(safe) > 180:
        import hashlib
        h = hashlib.sha256(safe.encode("utf-8")).hexdigest()
        safe = safe[:100] + "_" + h

    return os.path.join(_BOOSTS_CACHE_DIR, f"{safe}.json")


def _read_cache(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_cache(path: str, data) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Cache write failed %s: %s", path, e)


def _cache_valid(path: str, ttl: int) -> bool:
    if not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) < ttl


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

def _make_session():
    """
    Return a session suitable for OddsChecker access.

    Preference order:
      1) cloudscraper (recommended for Cloudflare mobile API)
      2) tls_client  (fallback if cloudscraper not installed)
      3) requests     (fallback; may be blocked by Cloudflare)
    """
    if cloudscraper:
        try:
            return cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False},
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

    logger.warning("Using requests.Session fallback (may be blocked by Cloudflare)")
    return requests.Session()


def _base_headers() -> dict[str, str]:
    """Common headers for all mobile-API requests."""
    headers = {
        "Content-Type": "application/json",
        "App-Type": "mapp",
        "Accept": "application/json",
        "Userbookmakers": ",".join(DEFAULT_BOOKMAKER_CODES[:1]) if DEFAULT_BOOKMAKER_CODES else "B3",
        "Device-Id": "32568159-9874-6184-B9E0-A21CADB4EB84",
        "Api-Key": OC_API_KEY or "",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "User-Agent": "Oddschecker/28716 CFNetwork/3826.500.111.2.2 Darwin/24.4.0",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    return headers


# ---------------------------------------------------------------------------
# Core GET helper
# ---------------------------------------------------------------------------

def _get(path: str, params: dict | None = None, cache_ttl: int | None = None) -> Any:
    """
    Perform a GET request against the OddsChecker mobile API.

    path       - URL path relative to OC_API_BASE, e.g. "/v1/bets-search"
    params     - query parameters dict
    cache_ttl  - seconds to cache the response (None = no caching)

    Returns the parsed JSON body, or None on failure.
    """
    _ensure_cache()

    url = f"{OC_API_BASE}{path}"
    cache_key = url + str(sorted((params or {}).items()))

    if cache_ttl is not None:
        cp = _cache_path(cache_key)
        if _cache_valid(cp, cache_ttl):
            logger.debug("Cache hit: %s", url)
            return _read_cache(cp)

    session = _make_session()
    try:
        logger.debug("GET %s params=%s", url, params)

        # tls_client uses timeout_seconds, requests uses timeout.
        get_kwargs = {
            "headers": _base_headers(),
            "params": params,
        }

        if tls_client and isinstance(session, tls_client.sessions.Session):
            get_kwargs["timeout_seconds"] = REQUEST_TIMEOUT
        else:
            get_kwargs["timeout"] = REQUEST_TIMEOUT

        resp = session.get(url, **get_kwargs)
        logger.debug("Response %s from %s", resp.status_code, url)

        if resp.status_code == 401:
            logger.error(
                "HTTP 401 from %s — the API may require an OC_API_KEY "
                "or the TLS fingerprint was rejected.  "
                "Set OC_API_KEY in your .env file if you have one.",
                url,
            )
            return None

        if resp.status_code == 403:
            logger.error("HTTP 403 from %s — access forbidden", url)
            return None

        if resp.status_code != 200:
            logger.error("HTTP %s from %s", resp.status_code, url)
            return None

        data = resp.json()

        if cache_ttl is not None:
            _write_cache(_cache_path(cache_key), data)

        return data

    except Exception as e:
        logger.exception("Request failed for %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------

def get_boosts(
    bet_type_ids: list[int] | None = None,
    bookmaker_codes: list[str] | None = None,
    category_group_ids: list[int] | None = None,
    event_ids: list[str] | None = None,
    subevent_ids: list[str] | None = None,
    minimum_odds: float = DEFAULT_MINIMUM_ODDS,
    offset: int = 0,
    size: int = DEFAULT_PAGE_SIZE,
    cache_ttl: int = CACHE_TTL_BOOSTS,
) -> list[dict]:
    """
    Fetch boosted/enhanced-price bets from /v1/bets-search.

    Parameters
    ----------
    bet_type_ids       - override the default boost bet-type IDs
    bookmaker_codes    - list of bookmaker shortcodes to include
    category_group_ids - sport category group IDs (2=football)
    event_ids          - restrict to specific event IDs (empty = all)
    subevent_ids       - restrict to specific subevent IDs (empty = all)
    minimum_odds       - minimum decimal odds threshold
    offset             - pagination offset
    size               - page size
    cache_ttl          - seconds to cache the response

    Returns
    -------
    List of boost dicts (empty list on failure).
    """
    if bet_type_ids is None:
        bet_type_ids = ALL_BOOST_BET_TYPE_IDS
    if bookmaker_codes is None:
        bookmaker_codes = DEFAULT_BOOKMAKER_CODES
    if category_group_ids is None:
        category_group_ids = [CATEGORY_GROUP_FOOTBALL]

    params = {
        "betTypeIds": ",".join(str(x) for x in bet_type_ids),
        "bookmakerCodes": ",".join(bookmaker_codes),
        "categoryGroupIds": ",".join(str(x) for x in category_group_ids),
        "eventIDs": ",".join(str(x) for x in (event_ids or [])),
        "subeventIds": ",".join(str(x) for x in (subevent_ids or [])),
        "minimumOdds": str(minimum_odds),
        "offset": str(offset),
        "size": str(size),
    }

    data = _get("/v1/bets-search", params=params, cache_ttl=cache_ttl)
    if data is None:
        return []

    # Normalise: the response may be a dict with a 'bets' key, or a list
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Common shapes: {"bets": [...]} or {"data": [...]} or {"results": [...]}
        for key in ("bets", "data", "results", "items"):
            if key in data and isinstance(data[key], list):
                return data[key]
        # If none of the above, return the whole dict wrapped in a list so
        # callers always get a list
        logger.debug("Unexpected bets-search response shape; keys=%s", list(data.keys()))
        return [data]

    return []


def get_all_boosts_paginated(
    bet_type_ids: list[int] | None = None,
    bookmaker_codes: list[str] | None = None,
    category_group_ids: list[int] | None = None,
    minimum_odds: float = DEFAULT_MINIMUM_ODDS,
    max_pages: int = 10,
) -> list[dict]:
    """
    Fetch *all* boosted bets by walking through pages until exhausted.

    Returns the combined list across all pages.
    """
    all_bets: list[dict] = []
    for page in range(max_pages):
        offset = page * DEFAULT_PAGE_SIZE
        page_bets = get_boosts(
            bet_type_ids=bet_type_ids,
            bookmaker_codes=bookmaker_codes,
            category_group_ids=category_group_ids,
            minimum_odds=minimum_odds,
            offset=offset,
            size=DEFAULT_PAGE_SIZE,
            cache_ttl=CACHE_TTL_BOOSTS,
        )
        if not page_bets:
            break
        all_bets.extend(page_bets)
        if len(page_bets) < DEFAULT_PAGE_SIZE:
            break  # last page

    return all_bets


def get_subevents_hierarchy(
    bet_type_ids: list[int] | None = None,
    bookmaker_codes: list[str] | None = None,
    category_group_id: int = CATEGORY_GROUP_FOOTBALL,
    event_ids: list[str] | None = None,
    subevent_ids: list[str] | None = None,
    cache_ttl: int = CACHE_TTL_BOOSTS,
) -> dict | None:
    """
    Fetch the event / subevent hierarchy from /v1/subevents-hierarchy.
    Useful for enriching boost data with full event names and metadata.

    Returns the raw API dict, or None on failure.
    """
    if bet_type_ids is None:
        bet_type_ids = ALL_BOOST_BET_TYPE_IDS
    if bookmaker_codes is None:
        bookmaker_codes = DEFAULT_BOOKMAKER_CODES

    params = {
        "betTypeIds": ",".join(str(x) for x in bet_type_ids),
        "bookmakerCodes": ",".join(bookmaker_codes),
        "categoryGroupId": str(category_group_id),
        "eventIDs": ",".join(str(x) for x in (event_ids or [])),
        "subeventIds": ",".join(str(x) for x in (subevent_ids or [])),
    }

    return _get("/v1/subevents-hierarchy", params=params, cache_ttl=cache_ttl)


def get_categories(cache_ttl: int = 3600) -> list[dict]:
    """Fetch all OddsChecker sport categories."""
    data = _get("/v1/categories", cache_ttl=cache_ttl)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    for key in ("categories", "data", "results"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


def get_bookmakers(cache_ttl: int = 3600) -> list[dict]:
    """Fetch all bookmaker definitions from OddsChecker."""
    data = _get("/v1/bookmakers", cache_ttl=cache_ttl)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    for key in ("bookmakers", "data", "results"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


def get_exchange_data(
    url: str = "https://api.oddsmatcha.uk/enhanced_specials/?active_only=true&odds_drops_only=false",
    timeout_seconds: int = 30,
) -> list[dict]:
    """Fetch enhanced exchange specials from oddsmatcha."""
    try:
        resp = requests.get(url, timeout=timeout_seconds)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
            return data["results"]
    except Exception as e:
        logger.warning("Failed to fetch exchange data: %s", e)
    return []


def _exchange_canonical_key(exchange_item: dict) -> str:
    market_type = exchange_item.get("market_type") or ""
    bet_description = exchange_item.get("bet_description") or ""
    event_name = exchange_item.get("event_name") or ""

    generic_markets = {"enhanced specials", "winner", "unknown", ""}

    if market_type.strip().lower() in generic_markets:
        candidate = _normalize_bet_name(bet_description)
    else:
        candidate = _normalize_bet_name(market_type)

    if not candidate:
        candidate = _normalize_bet_name(f"{event_name} {market_type} {bet_description}")

    return _canonicalize_text(candidate)


def _boost_canonical_bet_key(boost: dict) -> str:
    betname = boost.get("betName") or boost.get("name") or boost.get("selectionName") or boost.get("outcome") or ""
    return _canonicalize_text(_normalize_bet_name(betname))


def merge_exchange_data(boosts: list[dict], exchange_items: list[dict]) -> list[dict]:
    """Attach exchange info to matched boosts in-place."""
    if not boosts or not exchange_items:
        return boosts

    boost_index: dict[str, list[dict]] = {}
    for b in boosts:
        key = _boost_canonical_bet_key(b)
        if key:
            boost_index.setdefault(key, []).append(b)

    for item in exchange_items:
        key = _exchange_canonical_key(item)
        matched_boosts = boost_index.get(key, [])

        if not matched_boosts:
            # fallback: substring similarity for close text
            ex_norm = key
            for bkey, bvals in boost_index.items():
                if not bkey:
                    continue
                if ex_norm in bkey or bkey in ex_norm:
                    matched_boosts.extend(bvals)

        if not matched_boosts:
            continue

        ex_data = {
            "name": item.get("bet_description") or "",
            "exchangeName": item.get("exchange_name"),
            "back_odds": str(item.get("back_odds")) if item.get("back_odds") is not None else None,
            "lay_odds": str(item.get("lay_odds")) if item.get("lay_odds") is not None else None,
            "oddsUs": None,
            "direct_url": item.get("direct_url"),
        }

        for b in matched_boosts:
            b.setdefault("exchanges", []).append(ex_data)

    return boosts


def get_bookmaker_mapping_from_api(cache_ttl: int = 3600) -> dict[str, str]:
    """Fetch looking-glass bookmaker code->display-name mapping from OddsChecker."""
    raw = get_bookmakers(cache_ttl=cache_ttl)
    mapping: dict[str, str] = {}

    for m in raw or []:
        code = m.get("code") or m.get("bookmakerCode") or m.get("id")
        name = m.get("name") or m.get("bookmakerName")
        if code and name:
            mapping[str(code)] = str(name)

    return mapping


def save_bookmaker_mapping(mapping: dict[str, str], path: str = "bookmakers.json") -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning("Failed to save bookmaker mapping to %s: %s", path, e)


def load_bookmaker_mapping(path: str = "bookmakers.json") -> dict[str, str]:
    if not os.path.isfile(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
    except Exception as e:
        logger.warning("Failed to load bookmaker mapping from %s: %s", path, e)

    return {}


def refresh_bookmaker_mapping(path: str = "bookmakers.json", cache_ttl: int = 3600) -> dict[str, str]:
    """Fetch the latest bookmaker list and persist to a local mapping file."""
    upstream = get_bookmaker_mapping_from_api(cache_ttl=cache_ttl)
    final = {**BOOKMAKER_MAPPING, **upstream}

    save_bookmaker_mapping(final, path)
    return final


def get_big_football_matches(size: int = 10, cache_ttl: int = 300) -> list[dict]:
    """Fetch upcoming high-profile football matches."""
    data = _get("/football/v1/big-matches", params={"size": str(size)}, cache_ttl=cache_ttl)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    for key in ("matches", "events", "data", "results"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


def get_horse_racing_next_off(
    categories: str = "ukireland",
    size: int = 10,
    cache_ttl: int = 60,
) -> list[dict]:
    """Fetch next-off horse-racing events."""
    params = {"categories": categories, "size": str(size)}
    data = _get("/horse-racing/v3/next-off", params=params, cache_ttl=cache_ttl)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    for key in ("races", "events", "data", "results"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


def get_most_backed_bets(
    category_group_ids: list[int] | None = None,
    category_ids: list[int] | None = None,
    size: int = 20,
    cache_ttl: int = 120,
) -> list[dict]:
    """Fetch most-backed bets across selected categories."""
    params: dict[str, str] = {"size": str(size)}
    if category_group_ids:
        params["categoryGroupIds"] = ",".join(str(x) for x in category_group_ids)
    if category_ids:
        params["categoryIds"] = ",".join(str(x) for x in category_ids)

    data = _get("/v1/most-backed-bets", params=params, cache_ttl=cache_ttl)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    for key in ("bets", "data", "results"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


# ---------------------------------------------------------------------------
# Boost enrichment helpers
# ---------------------------------------------------------------------------

def _bookie_name(code: str) -> str:
    return BOOKMAKER_MAPPING.get(code, code)


def enrich_boosts_with_hierarchy(
    boosts: list[dict],
    hierarchy: dict | list | None,
) -> list[dict]:
    """
    Merge event/subevent display names from the subevents-hierarchy response
    into each boost dict (in-place update, also returns the list).

    The hierarchy payload structure is not fully known without a live response,
    so this attempts common field paths and falls back gracefully.
    """
    if not hierarchy or not boosts:
        return boosts

    subevent_map: dict[str, dict] = {}

    def _walk(node):
        if isinstance(node, dict):
            sid = node.get("subeventId") or node.get("id")
            if sid is not None:
                subevent_map[str(sid)] = node
            for value in node.values():
                _walk(value)

        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(hierarchy)

    for boost in boosts:
        sid = str(boost.get("subeventId", ""))
        if sid and sid in subevent_map:
            se = subevent_map[sid]
            boost.setdefault("eventName", se.get("name") or se.get("eventName", ""))
            boost.setdefault("eventId", se.get("eventId") or se.get("id") or boost.get("eventId"))
            boost.setdefault("subeventId", se.get("subeventId") or se.get("id") or boost.get("subeventId"))
            boost.setdefault("startTime", se.get("startTime") or se.get("startDate", ""))

    return boosts


def load_filters(filters_path: str = "filters.json") -> dict:
    """Load filter definitions from a JSON file."""
    if not os.path.isfile(filters_path):
        return {}

    try:
        with open(filters_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            logger.warning("Filters file %s is not a JSON object", filters_path)
    except Exception as e:
        logger.warning("Failed to load filters from %s: %s", filters_path, e)

    return {}


def _normalize(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip().lower()
    return value


def apply_filters(boosts: list[dict] | None, filters: dict) -> list[dict]:
    """Apply sparse include filters to boost records."""
    if not boosts:
        return []

    if not filters:
        return boosts

    normalized_filters = {}
    for field, allowed in filters.items():
        if isinstance(allowed, list):
            normalized_filters[field] = {str(item).strip().lower() for item in allowed}
        else:
            normalized_filters[field] = {str(allowed).strip().lower()}

    filtered: list[dict] = []
    for boost in boosts:
        keep = True
        for field, allowed_set in normalized_filters.items():
            value = _normalize(boost.get(field))
            if value is None or value not in allowed_set:
                keep = False
                break

        if keep:
            filtered.append(boost)

    return filtered
    if not hierarchy or not boosts:
        return boosts

    # Build subevent lookup: subeventId -> subevent dict
    subevent_map: dict[str, dict] = {}

    def _walk(node):
        """Recursively find subevent nodes."""
        if isinstance(node, dict):
            sid = node.get("subeventId") or node.get("id")
            if sid:
                subevent_map[str(sid)] = node
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(hierarchy)

    for boost in boosts:
        sid = str(boost.get("subeventId", ""))
        if sid and sid in subevent_map:
            se = subevent_map[sid]
            boost.setdefault("eventName", se.get("name") or se.get("eventName", ""))
            boost.setdefault("startTime", se.get("startTime") or se.get("startDate", ""))

    return boosts


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_boost(boost: dict) -> str:
    """
    Return a human-readable single-line summary of a boost dict.

    Handles unknown field names gracefully so the code keeps working even
    when the actual API response has a different shape.
    """
    bookies = boost.get("bookmakers")
    if bookies and isinstance(bookies, list) and len(bookies) > 0:
        # Keep full bookmaker metadata in text output for deduped entries.
        bookmaker_entries = []
        for b in bookies:
            normalized = {
                "bookmakerCode": b.get("bookmakerCode") or b.get("bookmaker") or b.get("bookie"),
                "bookmakerName": b.get("bookmakerName") or _bookie_name(b.get("bookmakerCode") or b.get("bookmaker") or b.get("bookie") or ""),
                "oddsFractional": b.get("oddsFractional"),
                "oddsDecimal": b.get("odds") or b.get("oddsDecimal"),
                "oddsUs": b.get("oddsUs"),
                "priceType": b.get("priceType"),
                "bookmakerBetId": b.get("bookmakerBetId"),
            }
            bookmaker_entries.append(normalized)

        bookmaker_list_str = "[" + ", ".join(str(x) for x in bookmaker_entries) + "]"
        bookie = ""  # no prefix in final output
        boosted = ""
        original = ""
    else:
        bookmaker_list_str = ""
        bookie_raw = boost.get("bookmakerCode") or boost.get("bookmaker") or boost.get("bookie") or "?"
        bookie = _bookie_name(bookie_raw)
        boosted = boost.get("boostedOdds") or boost.get("odds") or boost.get("oddsDecimal") or ""
        original = boost.get("originalOdds") or boost.get("referenceOdds") or ""

    bet_name = (
        boost.get("betName")
        or boost.get("name")
        or boost.get("selectionName")
        or boost.get("outcome")
        or "Unknown selection"
    )

    subevent = boost.get("subeventName") or boost.get("subevent") or ""
    event = (
        boost.get("eventName")
        or boost.get("event")
        or boost.get("matchName")
        or ""
    )

    odds_str = str(boosted)
    if original:
        odds_str += f" (was {original})"

    market = (
        boost.get("betTypeName")
        or boost.get("marketName")
        or boost.get("betType")
        or ""
    )

    parts = []
    if subevent:
        parts.append(subevent)
    if event and event != subevent:
        parts.append(f"({event})")
    if market:
        parts.append(f"| {market}")
    parts.append(f"| {bet_name}")

    if bookmaker_list_str:
        # only include odds prefix when we want old style
        parts.append(f"@ {bookmaker_list_str}")
    else:
        if odds_str:
            parts.append(f"@ {odds_str}")

    return "  ".join(parts)


def _normalize_bet_name(name: str) -> str:
    """Normalize bet description tokens to canonical equivalents."""
    import re

    if not name:
        return ""

    t = name.strip().lower()

    # remove parenthetical metadata (keep for bookmaker output only)
    t = re.sub(r"\([^)]*\)", "", t)

    # normalize 'Anytime' to score and remove plain 'anytime'
    t = re.sub(r"\bto\s+score\s+anytime\b", "to score", t)
    t = re.sub(r"\banytime\b", "", t)

    # remove trailing/embedded "was <odds>" qualifiers
    t = re.sub(r"\bwas\s+\d+(?:\.\d+)?(?:\/\d+)?\b", "", t)

    # team synonyms to normalized key; carefully avoid Man City
    t = re.sub(r"\bmanchester united\b|\bman utd\b|\bman united\b", "manunited", t)
    t = re.sub(r"\bmanchester city\b|\bman city\b", "mancity", t)

    # player-specific aliases / common name abbreviations
    t = re.sub(r"\bmatheus\s+cunha\b", "cunha", t)

    # shot-on-target canonicalization
    t = re.sub(r"\bplayer\s+shots\s+on\s+target\b", "shot on target", t)
    t = re.sub(r"\bshots\s+on\s+target\b", "shot on target", t)
    t = re.sub(r"\bshot\s+on\s+target\b", "shot on target", t)

    # odds quantifiers equivalence (1+ vs over 0.5)
    t = re.sub(r"1\+", "over0p5", t)
    t = re.sub(r"\bover\s*0\.?5\b", "over0p5", t)
    t = re.sub(r"\b0\.5\b", "0p5", t)

    return t


def _clean_bet_name_for_output(name: str) -> str:
    """Return a cleaned human-friendly bet name for merged output."""
    import re

    if not name:
        return ""

    out = name.strip()

    out = re.sub(r"\([^)]*\)", "", out)
    out = re.sub(r"\bwas\s+\d+(?:/\d+)?\b", "", out, flags=re.I)
    out = re.sub(r"\bto\s+score\s+anytime\b", "to score", out, flags=re.I)
    out = re.sub(r"\banytime\b", "", out, flags=re.I)

    out = re.sub(r"\s+", " ", out).strip()

    # strip trailing punctuation and non-alphanumeric fragments left from cleanup
    out = re.sub(r"[^A-Za-z0-9 ]+$", "", out).strip()

    return out


def _canonicalize_text(text: str) -> str:
    """Canonicalize text for deduping equivalent bets."""
    import re

    if not text:
        return ""

    t = text.strip().lower()
    t = t.replace("&", "and")
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    t = re.sub(r"\s+", " ", t)

    # remove filler words that produce near-equivalent outcomes
    fillers = ["was", "and", "the", "to", "have", "a", "in", "on", "each"]
    parts = sorted(set(w for w in t.split() if w not in fillers))
    return " ".join(parts)


def get_boost_canonic_key(boost: dict) -> str:
    """Return a canonical key representing the core bet description."""
    subevent = boost.get("subeventName", "")
    betname = _normalize_bet_name(boost.get("betName", "") or "")
    market = boost.get("marketName", "")

    canon = _canonicalize_text(f"{subevent} {market} {betname}")
    return canon


def dedupe_boosts(boosts: list[dict]) -> list[dict]:
    """Merge duplicate boosts across bookmakers into a canonical set."""
    if not boosts:
        return []

    dedup = {}
    for boost in boosts:
        key = get_boost_canonic_key(boost)

        bookie_entry = {
            "name": boost.get("betName") or boost.get("name") or boost.get("selectionName") or boost.get("outcome"),
            "betName": boost.get("betName") or boost.get("name") or boost.get("selectionName") or boost.get("outcome"),
            "bookmakerCode": boost.get("bookmakerCode") or boost.get("bookie") or boost.get("bookmaker"),
            "bookmakerName": _bookie_name(boost.get("bookmakerCode") or boost.get("bookie") or boost.get("bookmaker", "?")),
            "odds": boost.get("odds") or boost.get("oddsDecimal") or boost.get("boostedOdds") or "",
            "oddsFractional": boost.get("oddsFractional"),
            "oddsUs": boost.get("oddsUs"),
            "priceType": boost.get("priceType"),
            "bookmakerBetId": boost.get("bookmakerBetId"),
        }

        if key not in dedup:
            dedup[key] = boost.copy()
            dedup[key]["bookmakers"] = [bookie_entry]
        else:
            existing = dedup[key]
            existing_bookmakers = existing.get("bookmakers") or []
            existing_bookmakers.append(bookie_entry)
            existing["bookmakers"] = existing_bookmakers

    # Convert to list preserving original order by first appearance
    result = []
    for boost in boosts:
        key = get_boost_canonic_key(boost)
        if key in dedup:
            result.append(dedup.pop(key))

    return result


def format_boosts(boosts: list[dict]) -> str:
    """Format a list of boosts as a multi-line string."""
    if not boosts:
        return "  (no boosts found)"
    lines = [format_boost(b) for b in boosts]
    return "\n".join(lines)


def group_boosts_by_fixture(boosts: list[dict]) -> dict[str, list[dict]]:
    """Group boosts by fixture (subeventName/eventName/matchName)."""
    groups: dict[str, list[dict]] = {}
    for boost in boosts:
        fixture = (
            boost.get("subeventName")
            or boost.get("eventName")
            or boost.get("event")
            or boost.get("matchName")
            or "Unknown fixture"
        )
        groups.setdefault(fixture, []).append(boost)
    return groups


def format_boosts_grouped_by_fixture(boosts: list[dict]) -> str:
    """Return formatted grouped by fixture output."""
    if not boosts:
        return "  (no boosts found)"

    groups = group_boosts_by_fixture(boosts)
    parts: list[str] = []
    for fixture_name in sorted(groups):
        parts.append(f"{fixture_name}")
        group_lines = ["  " + l for l in format_boosts(groups[fixture_name]).split("\n")]
        parts.extend(group_lines)
        parts.append("")

    return "\n".join(parts).strip()


def build_boost_hierarchy(boosts: list[dict]) -> dict:
    """Build a nested event/fixture/bet hierarchy for JSON output."""
    hierarchy: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
    for boost in boosts:
        event_name = (
            boost.get("eventName")
            or boost.get("event")
            or "Unknown event"
        )
        fixture_name = (
            boost.get("subeventName")
            or boost.get("eventName")
            or boost.get("event")
            or boost.get("matchName")
            or "Unknown fixture"
        )
        raw_bet_name = (
            boost.get("betName")
            or boost.get("name")
            or boost.get("selectionName")
            or boost.get("outcome")
            or "Unknown bet"
        )
        bet_name = _clean_bet_name_for_output(raw_bet_name) or raw_bet_name

        market = (
            boost.get("betTypeName")
            or boost.get("marketName")
            or boost.get("betType")
            or ""
        )

        bookies_raw = boost.get("bookmakers")
        if not bookies_raw:
            bookie_code = boost.get("bookmakerCode") or boost.get("bookmaker") or boost.get("bookie")
            bookies_raw = [
                {
                    "bookmakerCode": bookie_code,
                    "bookmakerName": boost.get("bookmakerName"),
                    "odds": [
                        {
                            "bookieCode": boost.get("bookieCode"),
                            "oddsFractional": boost.get("oddsFractional"),
                            "oddsDecimal": boost.get("odds") or boost.get("oddsDecimal") or boost.get("boostedOdds"),
                            "oddsUs": boost.get("oddsUs"),
                            "priceType": boost.get("priceType"),
                            "bookmakerBetId": boost.get("bookmakerBetId"),
                        }
                    ]
                }
            ]

        normalized_bookmakers = []
        for b in bookies_raw:
            odds = b.get("odds")
            primary = None
            if isinstance(odds, list) and odds:
                first = odds[0]
                if isinstance(first, dict):
                    primary = first

            bookmaker_code = b.get("bookmakerCode") or b.get("bookieCode") or (primary and primary.get("bookieCode"))
            raw_bookmaker_name = b.get("bookmakerName") or b.get("bookmaker") or ""
            normalized_bookmaker_name = (
                raw_bookmaker_name
                if raw_bookmaker_name and raw_bookmaker_name != "?"
                else _bookie_name(bookmaker_code or "")
            )
            normalized = {
                "name": b.get("name") or b.get("betName") or bet_name,
                "bookmakerCode": bookmaker_code,
                "bookmakerName": normalized_bookmaker_name,
                "oddsFractional": (primary and primary.get("oddsFractional")) or b.get("oddsFractional"),
                "oddsDecimal": (primary and primary.get("oddsDecimal")) or b.get("oddsDecimal") or b.get("odds"),
                "oddsUs": (primary and primary.get("oddsUs")) or b.get("oddsUs"),
                "priceType": (primary and primary.get("priceType")) or b.get("priceType"),
                "bookmakerBetId": (primary and primary.get("bookmakerBetId")) or b.get("bookmakerBetId"),
            }
            normalized_bookmakers.append(normalized)

        event_group = hierarchy.setdefault(event_name, [])

        fixture_block = next(
            (f for f in event_group if f.get("fixture") == fixture_name),
            None,
        )

        if not fixture_block:
            fixture_block = {
                "fixture": fixture_name,
                "eventName": event_name,
                "eventId": boost.get("eventId"),
                "subeventId": boost.get("subeventId"),
                "startTime": boost.get("startTime"),
                "boosts": [],
            }
            event_group.append(fixture_block)

        boost_entry: dict[str, Any] = {
            "name": bet_name,
            "market": market,
            "bookmakers": normalized_bookmakers,
        }
        if boost.get("exchanges"):
            boost_entry["exchanges"] = boost["exchanges"]
        fixture_block["boosts"].append(boost_entry)

    return hierarchy


# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------

def run_boost_loop(
    poll_interval: int = 60,
    bet_type_ids: list[int] | None = None,
    bookmaker_codes: list[str] | None = None,
    category_group_ids: list[int] | None = None,
    minimum_odds: float = DEFAULT_MINIMUM_ODDS,
    include_hierarchy: bool = True,
    sport: str = "football",
) -> None:
    """
    Continuously poll OddsChecker for boosted bets and print new ones.

    poll_interval  - seconds between refreshes
    sport          - "football" or "horse_racing" (affects which bet-type IDs
                     and category groups are used by default)
    """
    if sport == "horse_racing":
        from config import BOOST_BET_TYPE_IDS_RACING, CATEGORY_GROUP_HORSE_RACING
        bet_type_ids = bet_type_ids or BOOST_BET_TYPE_IDS_RACING
        category_group_ids = category_group_ids or [CATEGORY_GROUP_HORSE_RACING]
    else:
        bet_type_ids = bet_type_ids or BOOST_BET_TYPE_IDS_FOOTBALL
        category_group_ids = category_group_ids or [CATEGORY_GROUP_FOOTBALL]

    seen_ids: set[str] = set()
    logger.info("Starting boost loop (sport=%s, interval=%ds)", sport, poll_interval)

    while True:
        try:
            boosts = get_all_boosts_paginated(
                bet_type_ids=bet_type_ids,
                bookmaker_codes=bookmaker_codes,
                category_group_ids=category_group_ids,
                minimum_odds=minimum_odds,
            )

            if include_hierarchy and boosts:
                hierarchy = get_subevents_hierarchy(
                    bet_type_ids=bet_type_ids,
                    bookmaker_codes=bookmaker_codes,
                    category_group_id=category_group_ids[0],
                )
                boosts = enrich_boosts_with_hierarchy(boosts, hierarchy)

            # Identify new boosts by a composite key
            new_boosts = []
            for b in boosts:
                bid = (
                    str(b.get("id") or "")
                    + str(b.get("betId") or "")
                    + str(b.get("bookmakerCode") or b.get("bookie") or "")
                    + str(b.get("boostedOdds") or b.get("odds") or "")
                )
                if bid not in seen_ids:
                    seen_ids.add(bid)
                    new_boosts.append(b)

            if new_boosts:
                print(f"\n{'='*60}")
                print(f"  {len(new_boosts)} new boost(s) — {_now()}")
                print(f"{'='*60}")
                print(format_boosts(new_boosts))
            else:
                print(f"[{_now()}] No new boosts (total fetched: {len(boosts)})")

        except KeyboardInterrupt:
            logger.info("Boost loop stopped by user.")
            break
        except Exception as e:
            logger.exception("Error in boost loop: %s", e)

        time.sleep(poll_interval)


def _now() -> str:
    import datetime
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
