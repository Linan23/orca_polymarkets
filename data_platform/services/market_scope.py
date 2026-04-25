"""Shared market-scope helpers for focused crawling and pruning."""

from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
import re
from typing import Any


DEFAULT_FOCUS_DOMAINS: tuple[str, ...] = (
    "politics",
    "crypto",
    "technology",
    "video-games",
)

FOCUS_DOMAIN_ALIASES: dict[str, str] = {
    "politics": "politics",
    "political": "politics",
    "geopolitics": "politics",
    "geopolitical": "politics",
    "geopolitics/political": "politics",
    "crypto": "crypto",
    "cryptocurrency": "crypto",
    "cryptocurrencies": "crypto",
    "technology": "technology",
    "tech": "technology",
    "video-games": "video-games",
    "video game": "video-games",
    "video games": "video-games",
    "videogame": "video-games",
    "videogames": "video-games",
    "gaming": "video-games",
}

DOMAIN_KEYWORDS: dict[str, tuple[str, ...]] = {
    "politics": (
        "politic",
        "political",
        "geopolit",
        "election",
        "elections",
        "government",
        "president",
        "presidential",
        "prime minister",
        "congress",
        "senate",
        "parliament",
        "parliamentary",
        "cabinet",
        "minister",
        "tariff",
        "foreign policy",
        "diplom",
        "ceasefire",
        "military",
        "nato",
        "ukraine",
        "russia",
        "putin",
        "zelensky",
        "china",
        "taiwan",
        "iran",
        "israel",
        "gaza",
        "syria",
        "starmer",
        "trump",
        "biden",
        "macron",
        "middle east",
        "world affairs",
        "global elections",
        "supreme court",
        "us government",
    ),
    "crypto": (
        "crypto",
        "cryptocurrency",
        "bitcoin",
        "ethereum",
        "solana",
        "doge",
        "dogecoin",
        "xrp",
        "token",
        "tokens",
        "airdrop",
        "coinbase",
        "kraken",
        "stablecoin",
        "microstrategy",
        "mstr",
        "blockchain",
        "defi",
    ),
    "technology": (
        "technology",
        "tech",
        "openai",
        "gpt",
        "llm",
        "artificial intelligence",
        "nvidia",
        "amd",
        "microsoft",
        "google",
        "alphabet",
        "meta",
        "apple",
        "anthropic",
        "sam altman",
        "semiconductor",
        "chip",
        "chips",
        "software",
        "hardware",
    ),
    "video-games": (
        "video game",
        "video games",
        "videogame",
        "videogames",
        "gaming",
        "gta",
        "grand theft auto",
        "nintendo",
        "playstation",
        "xbox",
        "steam",
        "esports",
        "esport",
        "counter-strike",
        "counter strike",
        "cs2",
        "fortnite",
        "call of duty",
        "rockstar",
        "rocket league",
        "league of legends",
        "lcs",
        "valorant",
        "dota",
        "team liquid",
        "100 thieves",
    ),
}

SUBSTRING_KEYWORDS: frozenset[str] = frozenset(
    {
        "politic",
        "geopolit",
        "diplom",
        "election",
        "government",
        "president",
        "parliament",
    }
)

DOMAIN_REGEXES: dict[str, tuple[re.Pattern[str], ...]] = {
    "crypto": (
        re.compile(r"(?<![a-z])btc(?![a-z])"),
        re.compile(r"(?<![a-z])eth(?![a-z])"),
        re.compile(r"(?<![a-z])sol(?![a-z])"),
        re.compile(r"kxbtc"),
        re.compile(r"kxeth"),
        re.compile(r"kxsol"),
        re.compile(r"btc-"),
        re.compile(r"eth-"),
        re.compile(r"sol-"),
    ),
    "technology": (
        re.compile(r"(?<![a-z])ai(?![a-z])"),
    ),
    "video-games": (
        re.compile(r"esports"),
    ),
}

VIDEO_GAMES_COMPARATOR_REGEXES: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bbefore\s+gta\s*vi\b"),
    re.compile(r"\bbefore\s+grand\s+theft\s+auto\s+vi\b"),
    re.compile(r"\bwhat\s+will\s+happen\s+before\s+gta\s*vi\b"),
    re.compile(r"\bwhat\s+will\s+happen\s+before\s+grand\s+theft\s+auto\s+vi\b"),
)

VIDEO_GAMES_DIRECT_TOPIC_REGEXES: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bgta[\s-]*vi\s+release"),
    re.compile(r"\brelease(?:d)?\b[^\n]*\bgta[\s-]*vi\b"),
    re.compile(r"\bgrand\s+theft\s+auto\s+vi\b[^\n]*(?:release|launch)"),
)

VIDEO_GAMES_STRONG_KEYWORDS: tuple[str, ...] = (
    "video game",
    "video games",
    "videogame",
    "videogames",
    "gaming",
    "nintendo",
    "playstation",
    "xbox",
    "steam",
    "esports",
    "counter-strike",
    "counter strike",
    "cs2",
    "fortnite",
    "call of duty",
    "rockstar",
    "rocket league",
    "league of legends",
)


@lru_cache(maxsize=None)
def _compile_keyword_regex(keyword: str) -> re.Pattern[str]:
    escaped = re.escape(keyword.casefold()).replace(r"\ ", r"\s+")
    return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])")


def _keyword_matches(haystack: str, keyword: str) -> bool:
    if keyword in SUBSTRING_KEYWORDS:
        return keyword in haystack
    return bool(_compile_keyword_regex(keyword).search(haystack))


def _video_games_matches(haystack: str) -> bool:
    keywords = DOMAIN_KEYWORDS.get("video-games", ())
    regexes = DOMAIN_REGEXES.get("video-games", ())
    if not any(_keyword_matches(haystack, keyword) for keyword in keywords) and not any(
        regex.search(haystack) for regex in regexes
    ):
        return False
    if not any(regex.search(haystack) for regex in VIDEO_GAMES_COMPARATOR_REGEXES):
        return True
    if any(regex.search(haystack) for regex in VIDEO_GAMES_DIRECT_TOPIC_REGEXES):
        return True
    return any(_keyword_matches(haystack, keyword) for keyword in VIDEO_GAMES_STRONG_KEYWORDS)


def add_focus_domain_argument(parser: Any) -> None:
    """Add a repeatable ``--focus-domain`` flag to an argparse parser."""
    parser.add_argument(
        "--focus-domain",
        action="append",
        default=[],
        help=(
            "Repeatable market scope filter. Supported domains: politics, crypto, technology, video-games. "
            "Aliases like geopolitics/political, cryptocurrency, tech, and 'video games' are accepted."
        ),
    )


def canonicalize_focus_domains(values: Iterable[str] | None) -> list[str]:
    """Normalize CLI/user-provided focus domains to canonical slugs."""
    if not values:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        raw = str(value or "").strip().casefold().replace("_", "-")
        if not raw:
            continue
        canonical = FOCUS_DOMAIN_ALIASES.get(raw)
        if canonical is None:
            available = ", ".join(DEFAULT_FOCUS_DOMAINS)
            raise ValueError(f"Unknown focus domain '{value}'. Supported domains: {available}.")
        if canonical not in seen:
            seen.add(canonical)
            normalized.append(canonical)
    return normalized


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().casefold()
    return text


def flatten_scope_texts(values: Iterable[Any]) -> list[str]:
    """Flatten nested scope text inputs into normalized strings."""
    texts: list[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            texts.extend(flatten_scope_texts(value))
            continue
        text = _coerce_text(value)
        if text:
            texts.append(text)
    return texts


def matched_focus_domains(texts: Iterable[Any], focus_domains: Iterable[str] | None) -> set[str]:
    """Return the canonical focus domains matched by the provided text corpus."""
    domains = canonicalize_focus_domains(focus_domains)
    if not domains:
        return set()
    haystack = "\n".join(flatten_scope_texts(texts))
    if not haystack:
        return set()
    matches: set[str] = set()
    for domain in domains:
        keywords = DOMAIN_KEYWORDS.get(domain, ())
        regexes = DOMAIN_REGEXES.get(domain, ())
        if domain == "video-games":
            if _video_games_matches(haystack):
                matches.add(domain)
            continue
        if any(_keyword_matches(haystack, keyword) for keyword in keywords) or any(
            regex.search(haystack) for regex in regexes
        ):
            matches.add(domain)
    return matches


def matches_focus_domains(texts: Iterable[Any], focus_domains: Iterable[str] | None) -> bool:
    """Return whether any requested focus domain matches the provided text corpus."""
    return bool(matched_focus_domains(texts, focus_domains))


def build_market_scope_texts(
    *,
    platform_name: str | None = None,
    event: Any | None = None,
    market: Any | None = None,
    tags: Iterable[Any] | None = None,
) -> list[str]:
    """Build a text corpus from normalized market/event rows."""
    texts: list[Any] = [platform_name]
    if event is not None:
        texts.extend(
            [
                getattr(event, "title", None),
                getattr(event, "slug", None),
                getattr(event, "category", None),
                getattr(event, "external_event_ref", None),
            ]
        )
    if market is not None:
        texts.extend(
            [
                getattr(market, "question", None),
                getattr(market, "market_slug", None),
                getattr(market, "market_url", None),
                getattr(market, "external_market_ref", None),
                getattr(market, "condition_ref", None),
                getattr(market, "outcome_a_label", None),
                getattr(market, "outcome_b_label", None),
            ]
        )
    if tags is not None and market is None:
        texts.extend(tags)
    return flatten_scope_texts(texts)


def polymarket_event_payload_matches_focus_domains(
    payload: dict[str, Any],
    focus_domains: Iterable[str] | None,
) -> bool:
    """Return whether a Polymarket event payload falls inside the requested scope."""
    if not canonicalize_focus_domains(focus_domains):
        return True
    texts: list[Any] = [
        payload.get("title"),
        payload.get("slug"),
        payload.get("category"),
        payload.get("ticker"),
    ]
    tags = payload.get("tags")
    if isinstance(tags, list):
        for tag in tags:
            if not isinstance(tag, dict):
                continue
            texts.extend([tag.get("label"), tag.get("slug")])
    markets = payload.get("markets")
    if isinstance(markets, list):
        for market in markets:
            if not isinstance(market, dict):
                continue
            texts.extend(
                [
                    market.get("question"),
                    market.get("slug"),
                    market.get("conditionId"),
                    market.get("outcomes"),
                ]
            )
    return matches_focus_domains(texts, focus_domains)


def polymarket_trade_payload_matches_focus_domains(
    payload: dict[str, Any],
    focus_domains: Iterable[str] | None,
) -> bool:
    """Return whether a Polymarket trade payload falls inside the requested scope."""
    if not canonicalize_focus_domains(focus_domains):
        return True
    texts = flatten_scope_texts(
        [
            payload.get("title"),
            payload.get("slug"),
            payload.get("eventSlug"),
            payload.get("outcome"),
            payload.get("asset"),
            payload.get("conditionId"),
        ]
    )
    return matches_focus_domains(texts, focus_domains)


def kalshi_trade_payload_matches_focus_domains(
    payload: dict[str, Any],
    focus_domains: Iterable[str] | None,
) -> bool:
    """Return whether a Kalshi trade payload falls inside the requested scope."""
    if not canonicalize_focus_domains(focus_domains):
        return True
    texts = flatten_scope_texts(
        [
            payload.get("ticker"),
            payload.get("title"),
            payload.get("subtitle"),
            payload.get("event_title"),
            payload.get("event_ticker"),
            payload.get("series_ticker"),
            payload.get("market"),
        ]
    )
    return matches_focus_domains(texts, focus_domains)
