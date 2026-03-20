"""
discord_notifier.py
====================
Sends Discord embeds summarising upcoming boosted fixtures.

Uses a bot token + channel ID (not a webhook).
State is persisted to a JSON file to prevent duplicate sends.

Public API
----------
  load_discord_config(path)        - load bot token / channel id from config.json
  send_fixture_embeds(hierarchy, mins, config, state_path)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import requests

from logging_config import get_logger

logger = get_logger("discord_notifier")

DISCORD_API = "https://discord.com/api/v10"

# Colour per exchange presence
COLOUR_WITH_EXCHANGE = 0x00B0F4   # blue — has exchange data
COLOUR_NO_EXCHANGE   = 0xF4A200   # amber — boosts only


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_discord_config(path: str = "config.json") -> dict:
    """
    Load Discord credentials from config.json.

    Expected format::

        {
          "discord": {
            "bot_token": "Bot YOUR_TOKEN_HERE",
            "channel_id": "123456789012345678"
          }
        }

    Returns the ``discord`` sub-dict, or an empty dict if unavailable.
    """
    if not os.path.isfile(path):
        logger.warning("config.json not found at %s", path)
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("discord", {})
    except Exception as e:
        logger.error("Failed to load discord config from %s: %s", path, e)
        return {}


# ---------------------------------------------------------------------------
# State (sent-fixture tracking)
# ---------------------------------------------------------------------------

def _load_state(path: str) -> dict:
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(path: str, state: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("Failed to save state to %s: %s", path, e)


# ---------------------------------------------------------------------------
# Discord API
# ---------------------------------------------------------------------------

def _post_embed(token: str, channel_id: str, embed: dict) -> bool:
    """POST a single embed to a Discord channel. Returns True on success."""
    url = f"{DISCORD_API}/channels/{channel_id}/messages"
    headers = {
        "Authorization": token if token.startswith("Bot ") else f"Bot {token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code in (200, 201):
            return True
        logger.error("Discord API error %s: %s", resp.status_code, resp.text[:200])
        return False
    except Exception as e:
        logger.error("Discord request failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Embed builder
# ---------------------------------------------------------------------------

def _minutes_until(start_time_str: str) -> float | None:
    """Return minutes until start_time_str (ISO 8601 UTC). None if unparseable."""
    if not start_time_str:
        return None
    try:
        # Handle both Z-suffix and +00:00 formats
        dt = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = (dt - now).total_seconds() / 60.0
        return delta
    except Exception:
        return None


def _boost_lines(boost: dict) -> str:
    """One or more text lines for a single boost: name + bookmaker odds."""
    name = boost.get("name", "Unknown")
    bookmakers = boost.get("bookmakers") or []

    parts = [f"**{name}**"]
    if not bookmakers:
        parts.append("\u00a0\u00a0—")
    else:
        for b in bookmakers:
            dec = b.get("oddsDecimal") or "—"
            bname = b.get("bookmakerName") or b.get("bookmakerCode") or ""
            parts.append(f"\u00a0\u00a0{dec} @ {bname}")
    return "\n".join(parts)


def _format_exchange_name(code: str | None) -> str:
    if not code:
        return ""
    code_lower = (code or "").strip().lower()
    if code_lower in {"matchbook", "mb"}:
        return "Matchbook"
    if code_lower in {"smarkets", "sm"}:
        return "Smarkets"
    if code_lower in {"betdaq", "bd", "beddaq"}:
        return "BetDAQ"
    return code


def _exchange_section(boosts: list[dict]) -> str:
    """Build the Exchange Markets section text. Returns empty string if none."""
    lines: list[str] = []
    for boost in boosts:
        for ex in (boost.get("exchanges") or []):
            raw_name   = ex.get("name") or ""
            event_name = ex.get("event_name") or ""
            back   = ex.get("back_odds") or "—"
            lay    = ex.get("lay_odds") or "—"
            exch   = _format_exchange_name(ex.get("exchangeName"))
            url    = ex.get("direct_url") or ""

            # Append event_name if it adds context not already in the name
            label = raw_name
            if event_name and event_name.lower() not in label.lower():
                label = f"{raw_name} ({event_name})"

            header = f"[{label}]({url})" if url else label
            lines.append(header)
            lines.append(f"\u00a0\u00a0{back} back / {lay} lay @ {exch}")

    return "\n".join(lines)


def _build_fixture_embed(fixture_block: dict) -> dict:
    """Build a Discord embed dict for a single fixture."""
    fixture_name = fixture_block.get("fixture", "Unknown fixture")
    start_time   = fixture_block.get("startTime", "")
    event_name   = fixture_block.get("eventName", "")
    boosts       = fixture_block.get("boosts") or []

    # Determine colour
    any_exchange = any(b.get("exchanges") for b in boosts)
    colour = COLOUR_WITH_EXCHANGE if any_exchange else COLOUR_NO_EXCHANGE

    # Boosts section
    boost_lines: list[str] = []
    for boost in boosts:
        boost_lines.append(_boost_lines(boost))
    boosts_text = "\n".join(boost_lines) if boost_lines else "_No boosts_"

    # Exchange section
    ex_text = _exchange_section(boosts)

    if ex_text:
        description = f"{boosts_text}\n\n**Exchange Markets Available**\n{ex_text}"
    else:
        description = boosts_text

    # Trim to Discord's 4096-char embed description limit
    if len(description) > 4000:
        description = description[:3990] + "\n…"

    embed: dict[str, Any] = {
        "title": fixture_name,
        "description": description,
        "color": colour,
        "footer": {"text": event_name},
    }

    if start_time:
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            embed["timestamp"] = dt.isoformat()
        except Exception:
            pass

    return embed


# ---------------------------------------------------------------------------
# Main public function
# ---------------------------------------------------------------------------

def send_fixture_embeds(
    hierarchy: dict,
    mins: int,
    config: dict,
    state_path: str = "discord_state.json",
) -> int:
    """
    For each fixture in hierarchy whose startTime is within ``mins`` minutes,
    send a Discord embed if not already sent.

    Returns the number of embeds sent.
    """
    token      = config.get("bot_token", "")
    channel_id = config.get("channel_id", "")

    if not token or not channel_id:
        logger.error("Discord bot_token or channel_id missing in config — skipping")
        return 0

    state = _load_state(state_path)
    sent  = 0

    for event_fixtures in hierarchy.values():
        for fixture_block in event_fixtures:
            fixture_key = f"{fixture_block.get('subeventId') or fixture_block.get('fixture')}"
            start_time  = fixture_block.get("startTime", "")
            time_until  = _minutes_until(start_time)

            if time_until is None:
                continue
            if time_until < 0 or time_until > mins:
                continue
            if state.get(fixture_key):
                logger.debug("Already sent for %s, skipping", fixture_key)
                continue

            embed = _build_fixture_embed(fixture_block)
            ok    = _post_embed(token, channel_id, embed)

            if ok:
                state[fixture_key] = {
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                    "fixture": fixture_block.get("fixture"),
                    "start_time": start_time,
                }
                _save_state(state_path, state)
                sent += 1
                logger.info("Sent embed for %s", fixture_block.get("fixture"))
            else:
                logger.warning("Failed to send embed for %s", fixture_block.get("fixture"))

    return sent
