from boosts_scraper import get_all_boosts_paginated, get_subevents_hierarchy, enrich_boosts_with_hierarchy, load_filters, apply_filters
from config import BOOST_BET_TYPE_IDS_FOOTBALL, DEFAULT_BOOKMAKER_CODES

boosts = get_all_boosts_paginated(
    bet_type_ids=BOOST_BET_TYPE_IDS_FOOTBALL,
    bookmaker_codes=DEFAULT_BOOKMAKER_CODES,
    category_group_ids=[2],
    minimum_odds=0.0,
)
print('initial', len(boosts))
filters = load_filters('filters.json')
print('filters', filters)
hierarchy = get_subevents_hierarchy(
    bet_type_ids=BOOST_BET_TYPE_IDS_FOOTBALL,
    bookmaker_codes=DEFAULT_BOOKMAKER_CODES,
    category_group_id=2,
)
print('hierarchy', type(hierarchy), len(hierarchy) if isinstance(hierarchy, dict) else None)
boosts = enrich_boosts_with_hierarchy(boosts, hierarchy)
filtered = apply_filters(boosts, filters)
print('after', len(filtered))
print('eventName set', sorted(set(x.get('eventName') for x in boosts if x.get('eventName')))[:20])
