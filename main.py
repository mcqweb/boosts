"""
main.py
=======
Entry point for the OddsChecker Boosts scraper.

Usage
-----
  # One-shot fetch of all current football boosts
  python main.py

  # Continuous polling every 30 seconds
  python main.py --loop --interval 30

  # Horse-racing boosts only
  python main.py --sport horse_racing

  # Football, specific bookmakers, minimum odds 2.0
  python main.py --bookmakers B3,PP,WH --min-odds 2.0

  # Dump raw JSON to a file
  python main.py --output boosts.json

  # Explore discovery endpoints (categories, bookmakers, big matches)
  python main.py --discover

Environment variables (see .env.example)
-----------------------------------------
  OC_API_KEY   - optional API key forwarded as X-Api-Key header
  LOG_LEVEL    - DEBUG / INFO / WARNING (default INFO)
  DEBUG_MODE   - set to "1" for verbose scraper debug output
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from logging_config import get_logger
from boosts_scraper import (
    apply_filters,
    build_boost_hierarchy,
    dedupe_boosts,
    format_boosts,
    format_boosts_grouped_by_fixture,
    get_all_boosts_paginated,
    get_big_football_matches,
    get_bookmakers,
    get_boosts,
    get_categories,
    get_horse_racing_next_off,
    get_most_backed_bets,
    get_subevents_hierarchy,
    enrich_boosts_with_hierarchy,
    load_filters,
    run_boost_loop,
)
from config import (
    ALL_BOOST_BET_TYPE_IDS,
    BOOST_BET_TYPE_IDS_FOOTBALL,
    BOOST_BET_TYPE_IDS_RACING,
    CATEGORY_GROUP_FOOTBALL,
    CATEGORY_GROUP_HORSE_RACING,
    DEFAULT_BOOKMAKER_CODES,
    DEFAULT_MINIMUM_ODDS,
)

logger = get_logger("main")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scrape OddsChecker for betting boosts / enhanced prices.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--sport",
        choices=["football", "horse_racing", "all"],
        default="football",
        help="Which sport to fetch boosts for.",
    )
    p.add_argument(
        "--bookmakers",
        default=",".join(DEFAULT_BOOKMAKER_CODES),
        help="Comma-separated bookmaker shortcodes, e.g. B3,PP,WH",
    )
    p.add_argument(
        "--min-odds",
        type=float,
        default=DEFAULT_MINIMUM_ODDS,
        dest="min_odds",
        help="Minimum decimal odds to include.",
    )
    p.add_argument(
        "--size",
        type=int,
        default=50,
        help="Results per API page.",
    )
    p.add_argument(
        "--loop",
        action="store_true",
        help="Run in continuous polling mode.",
    )
    p.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Poll interval in seconds (only with --loop).",
    )
    p.add_argument(
        "--output",
        metavar="FILE",
        default=None,
        help="Save raw boost JSON to this file.",
    )
    p.add_argument(
        "--discover",
        action="store_true",
        help="Fetch and display discovery endpoints (categories, bookmakers, big matches…).",
    )
    p.add_argument(
        "--no-hierarchy",
        action="store_true",
        dest="no_hierarchy",
        help="Skip the subevents-hierarchy call (faster, less event info).",
    )
    p.add_argument(
        "--raw",
        action="store_true",
        help="Print raw JSON instead of formatted output.",
    )
    p.add_argument(
        "--output-txt",
        metavar="FILE",
        default=None,
        help="Save formatted boost list to a plaintext file.",
    )
    p.add_argument(
        "--output-json",
        metavar="FILE",
        default="boosts_hierarchy.json",
        help="Save event/fixture/bet/bookie hierarchy as JSON.",
    )
    p.add_argument(
        "--no-exchange",
        action="store_true",
        dest="no_exchange",
        help="Skip fetching exchange specials (exchange data is included by default).",
    )
    p.add_argument(
        "--filters",
        metavar="FILE",
        default="filters.json",
        help="Load JSON filters from this file and apply to output.",
    )
    p.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Skip canonical deduplication (show all source boosts).",
    )
    p.add_argument(
        "--group-by",
        choices=["none", "fixture"],
        default="none",
        help="Group output by fixture (subevent/event).",
    )
    p.add_argument(
        "--sync-bookies",
        action="store_true",
        help="Fetch bookmakers mapping from OddsChecker and save to bookmakers.json (for scheduled refresh).",
    )
    p.add_argument(
        "--bookmaker-map",
        default="bookmakers.json",
        help="File path to save or load bookmaker mapping.",
    )
    p.add_argument(
        "--discord",
        action="store_true",
        help="Send Discord embeds for fixtures within --mins minutes of kick-off.",
    )
    p.add_argument(
        "--mins",
        type=int,
        default=60,
        metavar="MINS",
        help="Minutes before kick-off window for Discord notifications (default: 60).",
    )
    p.add_argument(
        "--config",
        default="config.json",
        help="Path to config.json containing Discord bot_token and channel_id (default: config.json).",
    )
    p.add_argument(
        "--discord-state",
        default="discord_state.json",
        dest="discord_state",
        help="State file used to track already-sent Discord notifications (default: discord_state.json).",
    )
    return p.parse_args()


def _bet_type_ids(sport: str) -> list[int]:
    if sport == "football":
        return BOOST_BET_TYPE_IDS_FOOTBALL
    if sport == "horse_racing":
        return BOOST_BET_TYPE_IDS_RACING
    return ALL_BOOST_BET_TYPE_IDS


def _category_groups(sport: str) -> list[int]:
    if sport == "football":
        return [CATEGORY_GROUP_FOOTBALL]
    if sport == "horse_racing":
        return [CATEGORY_GROUP_HORSE_RACING]
    return [CATEGORY_GROUP_FOOTBALL, CATEGORY_GROUP_HORSE_RACING]


def run_discover() -> None:
    """Fetch and print discovery endpoint data."""
    print("\n" + "=" * 60)
    print("  DISCOVERY MODE")
    print("=" * 60)

    print("\n--- Categories ---")
    cats = get_categories()
    if cats:
        for c in cats[:20]:
            print(f"  {c}")
    else:
        print("  (none returned / check auth)")

    print("\n--- Bookmakers ---")
    bkms = get_bookmakers()
    if bkms:
        for b in bkms[:20]:
            print(f"  {b}")
    else:
        print("  (none returned / check auth)")

    print("\n--- Big Football Matches ---")
    matches = get_big_football_matches(size=5)
    if matches:
        for m in matches:
            print(f"  {m}")
    else:
        print("  (none returned / check auth)")

    print("\n--- Horse Racing Next Off ---")
    races = get_horse_racing_next_off(size=5)
    if races:
        for r in races:
            print(f"  {r}")
    else:
        print("  (none returned / check auth)")

    print("\n--- Most Backed Bets (football) ---")
    backed = get_most_backed_bets(category_group_ids=[CATEGORY_GROUP_FOOTBALL], size=5)
    if backed:
        for b in backed:
            print(f"  {b}")
    else:
        print("  (none returned / check auth)")


def run_once(args: argparse.Namespace) -> None:
    """Fetch boosts once and display / save."""
    bookmaker_codes = [c.strip() for c in args.bookmakers.split(",") if c.strip()]
    bet_type_ids = _bet_type_ids(args.sport)
    category_groups = _category_groups(args.sport)

    print(f"\nFetching boosts: sport={args.sport}, bookmakers={bookmaker_codes}, "
          f"min_odds={args.min_odds}")

    boosts = get_all_boosts_paginated(
        bet_type_ids=bet_type_ids,
        bookmaker_codes=bookmaker_codes,
        category_group_ids=category_groups,
        minimum_odds=args.min_odds,
    )

    filters = load_filters(args.filters) if args.filters else {}

    if not args.no_hierarchy and boosts:
        print("Fetching subevents hierarchy for enrichment…")
        hierarchy = get_subevents_hierarchy(
            bet_type_ids=bet_type_ids,
            bookmaker_codes=bookmaker_codes,
            category_group_id=category_groups[0],
        )
        boosts = enrich_boosts_with_hierarchy(boosts, hierarchy)

    if boosts is None:
        boosts = []

    if filters:
        boosts = apply_filters(boosts, filters)

    before_dedupe = len(boosts)
    if not args.no_dedupe:
        boosts = dedupe_boosts(boosts)
    after_dedupe = len(boosts)

    if not args.no_exchange:
        from boosts_scraper import get_exchange_data, merge_exchange_data

        exchange_items = get_exchange_data()
        if exchange_items:
            boosts = merge_exchange_data(boosts, exchange_items)
            print(f"\nMerged {len(exchange_items)} exchange item(s) into boosts\n")

    if args.no_dedupe:
        print(f"\nFound {after_dedupe} boost(s) (dedupe disabled).\n")
    else:
        print(f"\nFound {before_dedupe} boost(s) before dedupe, {after_dedupe} after dedupe.\n")

    if args.raw:
        print(json.dumps(boosts, indent=2, ensure_ascii=False))
    else:
        if args.group_by == "fixture":
            output = format_boosts_grouped_by_fixture(boosts)
        else:
            output = format_boosts(boosts)
        print(output)

    if args.output:
        try:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(boosts, f, indent=2, ensure_ascii=False)
            print(f"\nRaw JSON saved to {args.output}")
        except Exception as e:
            logger.error("Failed to save output: %s", e)

    if args.output_txt:
        try:
            formatted = output if not args.raw else json.dumps(boosts, indent=2, ensure_ascii=False)
            with open(args.output_txt, "w", encoding="utf-8") as f:
                f.write(formatted)
            print(f"\nFormatted text saved to {args.output_txt}")
        except Exception as e:
            logger.error("Failed to save text output: %s", e)

    if args.output_json or args.discord:
        hierarchy = build_boost_hierarchy(boosts)

        if args.output_json:
            try:
                with open(args.output_json, "w", encoding="utf-8") as f:
                    json.dump(hierarchy, f, indent=2, ensure_ascii=False)
                print(f"\nHierarchical JSON saved to {args.output_json}")
            except Exception as e:
                logger.error("Failed to save JSON output: %s", e)

        if args.discord:
            from discord_notifier import load_discord_config, send_fixture_embeds
            discord_cfg = load_discord_config(args.config)
            sent = send_fixture_embeds(hierarchy, args.mins, discord_cfg, args.discord_state)
            print(f"\nDiscord: {sent} embed(s) sent (window: {args.mins} min).")


def main() -> None:
    args = parse_args()

    if args.discover:
        run_discover()
        return

    if args.sync_bookies:
        from boosts_scraper import refresh_bookmaker_mapping

        saved = refresh_bookmaker_mapping(path=args.bookmaker_map)
        print(f"Saved {len(saved)} bookmaker mappings to {args.bookmaker_map}")
        return

    if args.loop:
        run_boost_loop(
            poll_interval=args.interval,
            bet_type_ids=_bet_type_ids(args.sport),
            bookmaker_codes=[c.strip() for c in args.bookmakers.split(",") if c.strip()],
            category_group_ids=_category_groups(args.sport),
            minimum_odds=args.min_odds,
            include_hierarchy=not args.no_hierarchy,
            sport=args.sport,
        )
    else:
        run_once(args)


if __name__ == "__main__":
    main()
