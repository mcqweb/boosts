"""
Central configuration constants for the OddsChecker boosts scraper.

All tuneable parameters live here so the rest of the code stays clean.
Override values via environment variables where noted.
"""

import os

# ---------------------------------------------------------------------------
# OddsChecker API base
# ---------------------------------------------------------------------------
OC_API_BASE = "https://api.oddschecker.com/api/mobile-app"

# ---------------------------------------------------------------------------
# Authentication
# An optional API key can be supplied via the OC_API_KEY env-var.
# Many OddsChecker mobile-API endpoints also work purely via TLS-fingerprint
# spoofing (tls_client) without an explicit key — the key is tried first when
# present.
# ---------------------------------------------------------------------------
OC_API_KEY = os.getenv("OC_API_KEY", "a1d4634b-6cd8-4485-a7cd-c9b91f38177f")

# ---------------------------------------------------------------------------
# Bet-type IDs for "boosted" / "enhanced-price" markets
# ---------------------------------------------------------------------------

# Football / general sports boosts
BOOST_BET_TYPE_IDS_FOOTBALL = [
    605408,
    4401026,
    107319977,
    107319917,
]

# Horse-racing / boxing boosts (suspected — enable if needed)
BOOST_BET_TYPE_IDS_RACING = [
    70062005,
    97675625,
]

# Combined — used for the generic bets-search endpoint
ALL_BOOST_BET_TYPE_IDS = BOOST_BET_TYPE_IDS_FOOTBALL + BOOST_BET_TYPE_IDS_RACING

# ---------------------------------------------------------------------------
# Category group IDs  (OddsChecker "sport" groupings)
# ---------------------------------------------------------------------------
CATEGORY_GROUP_FOOTBALL = 2
CATEGORY_GROUP_HORSE_RACING = 3

# ---------------------------------------------------------------------------
# Bookmaker codes to include by default
# ---------------------------------------------------------------------------
DEFAULT_BOOKMAKER_CODES = ["B3", "FR", "PP", "WH", "BF", "SK", "LD", "UN",
                           "CE", "WA", "BY", "VC", "KN"]

# Mapping of shortcode → display name (kept in sync with oddschecker_client)
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

# ---------------------------------------------------------------------------
# Pagination / request defaults
# ---------------------------------------------------------------------------
DEFAULT_PAGE_SIZE = 50       # max results per request
DEFAULT_MINIMUM_ODDS = 0.0   # include all odds by default
REQUEST_TIMEOUT = 30         # seconds

# ---------------------------------------------------------------------------
# Local cache directory (relative to cwd)
# ---------------------------------------------------------------------------
CACHE_DIR = "./cache"
CACHE_TTL_BOOSTS = 60        # seconds to cache boost data
