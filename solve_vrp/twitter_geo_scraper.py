#!/usr/bin/env python3
"""
Compatibility scraper module migrated from Twitter/X to Bluesky.

This keeps the old public class/method names so existing integrations keep
working, while the data source is now Bluesky.
"""

from __future__ import annotations

import csv
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from solve_vrp.social_geo_scraper import SocialGeoScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bluesky_scraper.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class TwitterGeoScraper:
    """
    Backward-compatible class name.

    Data source is Bluesky now.
    """

    def __init__(
        self,
        bearer_token: str = "",
        bluesky_identifier: Optional[str] = None,
        bluesky_app_password: Optional[str] = None,
        bluesky_api_base: str = "https://api.bsky.app",
        timeout_sec: int = 10,
    ):
        self.bearer_token = str(bearer_token or "").strip()  # kept for compatibility
        self.bluesky_identifier = str(
            bluesky_identifier or os.getenv("BLUESKY_IDENTIFIER", "")
        ).strip()
        self.bluesky_app_password = str(
            bluesky_app_password or os.getenv("BLUESKY_APP_PASSWORD", "")
        ).strip()
        self.bluesky_api_base = str(bluesky_api_base or "https://api.bsky.app").strip()
        self.timeout_sec = max(3, int(timeout_sec))

        self.scraper = SocialGeoScraper(
            bluesky_api_base=self.bluesky_api_base,
            bluesky_identifier=self.bluesky_identifier,
            bluesky_app_password=self.bluesky_app_password,
            timeout_sec=self.timeout_sec,
        )

        self.locations = {
            "brussel": {"lat": 50.8427501, "lng": 4.3515499, "name": "Brussel, Belgica"},
            "anthisnes": {"lat": 50.4812987, "lng": 5.5198048, "name": "Anthisnes, Belgica"},
            "ave_et_auffe": {"lat": 50.1085676, "lng": 5.1429583, "name": "Ave-et-Auffe, Belgica"},
            "bavikhove": {"lat": 50.8753581, "lng": 3.3113744, "name": "Bavikhove, Belgica"},
            "bersillies_l_abbaye": {
                "lat": 50.2624949,
                "lng": 4.1523037,
                "name": "Bersillies-l'Abbaye, Belgica",
            },
            "bleret": {"lat": 50.688785, "lng": 5.2859129, "name": "Bleret, Belgica"},
            "bourlers": {"lat": 50.025208, "lng": 4.3409814, "name": "Bourlers, Belgica"},
            "bulskamp": {"lat": 51.0432467, "lng": 2.6501345, "name": "Bulskamp, Belgica"},
            "chievres": {"lat": 50.5661351, "lng": 3.7853169, "name": "Chievres, Belgica"},
            "damme": {"lat": 51.2365829, "lng": 3.3400765, "name": "Damme, Belgica"},
            "donstiennes": {"lat": 50.2849958, "lng": 4.3106966, "name": "Donstiennes, Belgica"},
            "elverdinge": {"lat": 50.8848669, "lng": 2.8162074, "name": "Elverdinge, Belgica"},
            "evelette": {"lat": 50.411922, "lng": 5.173705, "name": "Evelette, Belgica"},
            "fontaine_l_eveque": {
                "lat": 50.4100558,
                "lng": 4.3249526,
                "name": "Fontaine-l'Eveque, Belgica",
            },
            "gelinden": {"lat": 50.7670559, "lng": 5.2628934, "name": "Gelinden, Belgica"},
            "gondregnies": {"lat": 50.6271045, "lng": 3.9115699, "name": "Gondregnies, Belgica"},
            "guignies": {"lat": 50.5499808, "lng": 3.3727285, "name": "Guignies, Belgica"},
            "haren_brussel": {
                "lat": 50.8919578,
                "lng": 4.4182942,
                "name": "Haren (Brussel), Belgica",
            },
            "heppignies": {"lat": 50.4814121, "lng": 4.4932598, "name": "Heppignies, Belgica"},
            "hofstade_bt": {"lat": 50.9912912, "lng": 4.492735, "name": "Hofstade, Belgica"},
            "humbeek": {"lat": 50.9667658, "lng": 4.3834362, "name": "Humbeek, Belgica"},
            "jurbise": {"lat": 50.5208191, "lng": 3.9017078, "name": "Jurbise, Belgica"},
            "komen": {"lat": 50.77614, "lng": 3.007366, "name": "Komen (Comines), Belgica"},
            "landegem": {"lat": 51.0566021, "lng": 3.573045, "name": "Landegem, Belgica"},
            "les_bons_villers": {
                "lat": 50.5427462,
                "lng": 4.4476802,
                "name": "Les Bons Villers, Belgica",
            },
            "lissewege": {"lat": 51.2948381, "lng": 3.1994146, "name": "Lissewege, Belgica"},
            "maffle": {"lat": 50.6200815, "lng": 3.8018102, "name": "Maffle, Belgica"},
            "maulde": {"lat": 50.6165736, "lng": 3.5470846, "name": "Maulde, Belgica"},
            "merksplas": {"lat": 51.361787, "lng": 4.861625, "name": "Merksplas, Belgica"},
            "mont_nam": {"lat": 50.3534706, "lng": 4.9014191, "name": "Mont (Namur), Belgica"},
        }

        logger.info("Bluesky scraper configured.")

    def build_geographic_query(
        self,
        keywords: str,
        locations: List[str],
        radius_km: int = 10,
        minutes_back: int = 30,
    ) -> str:
        # Kept intentionally: the old OR city logic.
        city_keywords = []
        for location in locations:
            if location in self.locations:
                full_name = self.locations[location]["name"]
                city_name = full_name.split(",")[0].strip()
                city_keywords.append(city_name)
            elif str(location).startswith("custom:"):
                logger.warning(
                    "Custom coordinates are not used in Bluesky query mode: %s", location
                )
            else:
                logger.warning("Unknown location key: %s", location)

        city_query = f"({' OR '.join(city_keywords)})" if city_keywords else ""

        if city_query and keywords:
            final_query = f"({keywords}) {city_query}"
        elif city_query:
            final_query = city_query
        else:
            final_query = str(keywords or "").strip()

        # Kept for compatibility with previous output/query style.
        if final_query:
            final_query += " -is:retweet"
        return final_query.strip()

    def search_recent_tweets(
        self,
        query: str,
        max_results: int = 100,
        tweet_fields: Optional[List[str]] = None,
        user_fields: Optional[List[str]] = None,
        expansions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        requested_results = max(1, min(int(max_results or 10), 100))
        posts = self.scraper.search_recent_posts(
            location_name="",
            keywords=query,
            per_location_limit=requested_results,
            minutes_back=30,
        )
        rows = [self._post_to_row(post, "", "") for post in posts]
        return {
            "tweets": rows,
            "users": {},
            "places": {},
            "meta": {
                "requested_results": requested_results,
                "api_max_results": requested_results,
                "returned_results": len(rows),
                "source_platform": "bluesky",
            },
        }

    def search_recent_tweets_by_location(
        self,
        keywords: str,
        locations: Optional[List[str]] = None,
        per_location_limit: int = 5,
        radius_km: int = 10,
        minutes_back: int = 30,
        pause_seconds: float = 1.0,
    ) -> List[Dict[str, Any]]:
        if locations is None or len(locations) == 0:
            locations = list(self.locations.keys())

        per_location_limit = max(1, min(int(per_location_limit), 100))
        rows: List[Dict[str, Any]] = []

        for index, location_key in enumerate(locations, start=1):
            if location_key not in self.locations:
                logger.warning("Unknown location, skipping: %s", location_key)
                continue

            location_name = self.locations[location_key]["name"]
            logger.info("[%s/%s] Searching posts for %s", index, len(locations), location_name)

            query = self.build_geographic_query(
                keywords=keywords,
                locations=[location_key],
                radius_km=radius_km,
                minutes_back=minutes_back,
            )

            posts = self.scraper.search_recent_posts(
                location_name=location_name,
                keywords=query,
                per_location_limit=per_location_limit,
                minutes_back=minutes_back,
            )

            for post in posts[:per_location_limit]:
                rows.append(self._post_to_row(post, location_key, location_name))

            if pause_seconds > 0 and index < len(locations):
                time.sleep(pause_seconds)

        rows.sort(
            key=lambda x: (
                str(x.get("location_name", "")),
                str(x.get("created_at", "")),
            ),
            reverse=True,
        )
        return rows

    def process_results(
        self, search_results: Dict[str, Any], locations_used: List[str]
    ) -> List[Dict[str, Any]]:
        tweets = search_results.get("tweets", []) if isinstance(search_results, dict) else []
        rows: List[Dict[str, Any]] = []

        for tweet in tweets:
            if not isinstance(tweet, dict):
                continue
            row = dict(tweet)
            matched = row.get("matched_location") or self.determine_matched_location(
                row, locations_used
            )
            row["matched_location"] = matched

            created_at = _parse_utc_datetime(row.get("created_at"))
            if created_at is not None:
                now = datetime.now(timezone.utc)
                row["minutes_ago"] = int((now - created_at).total_seconds() / 60)
            else:
                row["minutes_ago"] = None

            rows.append(row)

        rows.sort(key=lambda x: str(x.get("created_at", "")), reverse=True)
        return rows

    def determine_matched_location(self, tweet_data: Dict[str, Any], locations_used: List[str]) -> str:
        tweet_text = str(tweet_data.get("text", "")).lower()
        user_location = str(tweet_data.get("user_location", "")).lower()
        place_name = str(tweet_data.get("place_name", "")).lower()

        for location in locations_used:
            if location in self.locations:
                location_name = self.locations[location]["name"]
                city_name = location_name.split(",")[0].lower()
                words = city_name.split()
                if (
                    city_name in place_name
                    or city_name in user_location
                    or city_name in tweet_text
                    or any(word in place_name for word in words)
                    or any(word in user_location for word in words)
                    or any(word in tweet_text for word in words)
                ):
                    return location_name

        if locations_used and locations_used[0] in self.locations:
            return self.locations[locations_used[0]]["name"]

        return "General location"

    def format_output(self, rows: List[Dict[str, Any]], search_config: Dict[str, Any]) -> str:
        if not rows:
            return self._format_empty_results(search_config)

        output: List[str] = []
        output.append("=" * 80)
        output.append("BLUESKY SCRAPING REPORT")
        output.append("=" * 80)
        output.append("")
        output.append("SEARCH CONFIGURATION:")
        output.append(f"    Keywords: {search_config.get('keywords', 'N/A')}")
        output.append(f"    Locations: {', '.join(search_config.get('locations', []))}")
        output.append(f"    Radius: {search_config.get('radius_km', 0)} km")
        output.append(f"    Time window: last {search_config.get('minutes_back', 0)} minutes")
        output.append(f"    Results found: {len(rows)}")
        output.append(f"    Executed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append("")

        usernames = {str(r.get("username", "")).strip() for r in rows if r.get("username")}
        output.append("SUMMARY:")
        output.append(f"   Locations with posts: {len({r.get('matched_location') for r in rows if r.get('matched_location')})}")
        output.append(f"   Unique users: {len(usernames)}")
        output.append(f"   Total likes: {sum(int(r.get('like_count') or 0) for r in rows)}")
        output.append(f"   Total reposts: {sum(int(r.get('retweet_count') or 0) for r in rows)}")
        output.append(f"   Total replies: {sum(int(r.get('reply_count') or 0) for r in rows)}")
        output.append("")

        location_counts: Dict[str, int] = {}
        for row in rows:
            key = str(row.get("matched_location") or row.get("location_name") or "Unknown")
            location_counts[key] = location_counts.get(key, 0) + 1

        output.append("DISTRIBUTION BY LOCATION:")
        for location, count in sorted(location_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(rows)) * 100
            output.append(f"   - {location}: {count} posts ({percentage:.1f}%)")

        output.append("")
        output.append("TOP 5 ACTIVE USERS:")
        user_counts: Dict[str, int] = {}
        for row in rows:
            username = str(row.get("username") or "").strip()
            if username:
                user_counts[username] = user_counts.get(username, 0) + 1
        for username, count in sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            output.append(f"   - @{username}: {count} posts")

        output.append("")
        output.append("=" * 80)
        output.append("POSTS (newest first)")
        output.append("=" * 80)

        for idx, row in enumerate(rows[:20], start=1):
            output.append("")
            output.append(f"POST #{idx}")
            output.append(f"   Time: {row.get('created_at', 'n/a')} ({row.get('minutes_ago', 'n/a')} min ago)")
            output.append(f"   User: @{row.get('username', 'unknown')}")
            output.append(f"   Location: {row.get('matched_location') or row.get('location_name') or 'n/a'}")
            output.append(
                f"   Metrics: {int(row.get('like_count') or 0)} likes, "
                f"{int(row.get('retweet_count') or 0)} reposts, "
                f"{int(row.get('reply_count') or 0)} replies"
            )
            text = str(row.get("text", ""))
            if len(text) > 220:
                text = text[:220] + "..."
            output.append(f"   Text: {text}")
            if row.get("tweet_url"):
                output.append(f"   URL: {row['tweet_url']}")

        if len(rows) > 20:
            output.append("")
            output.append(f"... and {len(rows) - 20} additional posts")

        output.append("")
        output.append("=" * 80)
        output.append(f"Report generated successfully - {len(rows)} posts processed")
        output.append("=" * 80)
        return "\n".join(output)

    def _format_empty_results(self, search_config: Dict[str, Any]) -> str:
        output = []
        output.append("=" * 80)
        output.append("BLUESKY SCRAPING REPORT")
        output.append("=" * 80)
        output.append("")
        output.append("NO POSTS FOUND")
        output.append("")
        output.append("CONFIGURATION USED:")
        output.append(f"    Keywords: {search_config.get('keywords', 'N/A')}")
        output.append(f"    Locations: {', '.join(search_config.get('locations', []))}")
        output.append(f"    Radius: {search_config.get('radius_km', 0)} km")
        output.append(f"    Time window: last {search_config.get('minutes_back', 0)} minutes")
        output.append("")
        output.append("SUGGESTIONS:")
        output.append("   - Increase the time window (60-120 minutes)")
        output.append("   - Use broader keywords")
        output.append("   - Add more location variants")
        output.append("   - Verify Bluesky credentials if using authenticated mode")
        output.append("")
        output.append("=" * 80)
        return "\n".join(output)

    def save_results(self, rows: List[Dict[str, Any]], filename: str = "") -> str:
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bluesky_scraping_{timestamp}.csv"

        if not rows:
            logger.warning("No rows to save.")
            return ""

        fieldnames = sorted({key for row in rows for key in row.keys()})
        try:
            with open(filename, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            logger.info("Results saved to %s", filename)
            return filename
        except Exception as exc:  # noqa: BLE001
            logger.error("Error saving CSV: %s", exc)
            return ""

    def _post_to_row(
        self, post: Dict[str, Any], location_key: str, location_name: str
    ) -> Dict[str, Any]:
        row = dict(post)
        row["location_key"] = location_key
        row["location_name"] = location_name
        row["matched_location"] = location_name or "General location"
        created_at = _parse_utc_datetime(post.get("created_at"))
        if created_at is not None:
            now = datetime.now(timezone.utc)
            row["minutes_ago"] = int((now - created_at).total_seconds() / 60)
        else:
            row["minutes_ago"] = None
        row.setdefault("user_name", row.get("username"))
        row.setdefault("user_location", None)
        row.setdefault("place_name", None)
        return row


BlueskyGeoScraper = TwitterGeoScraper


def _parse_utc_datetime(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def setup_credentials() -> Dict[str, str]:
    identifier = str(os.getenv("BLUESKY_IDENTIFIER", "")).strip()
    app_password = str(os.getenv("BLUESKY_APP_PASSWORD", "")).strip()

    if not identifier:
        identifier = input("Bluesky identifier (handle or DID): ").strip()
    if not app_password:
        app_password = input("Bluesky app password: ").strip()

    if not identifier or not app_password:
        raise ValueError("BLUESKY_IDENTIFIER and BLUESKY_APP_PASSWORD are required")

    return {"identifier": identifier, "app_password": app_password}


def main() -> None:
    print("Starting Bluesky geo-temporal scraper")
    print("=" * 60)

    try:
        creds = setup_credentials()
        scraper = TwitterGeoScraper(
            bluesky_identifier=creds["identifier"],
            bluesky_app_password=creds["app_password"],
        )

        keywords = input(
            "Keywords (example: 'accident OR robbery OR protest') [default set]: "
        ).strip()
        if not keywords:
            keywords = (
                "accident OR robbery OR protest OR fire OR risk OR alert OR emergency "
                "OR flood OR storm OR incident"
            )

        mode_input = input("Search mode: all or custom [all]: ").strip().lower()
        use_all_locations = mode_input in {"", "all"}

        print("\nAvailable locations:")
        for key, info in scraper.locations.items():
            print(f"   - {key}: {info['name']}")

        if use_all_locations:
            locations = list(scraper.locations.keys())
            per_location_input = input("Max posts per location [5]: ").strip()
            per_location_limit = int(per_location_input) if per_location_input else 5
            per_location_limit = max(1, min(per_location_limit, 100))
            max_results = per_location_limit
        else:
            location_input = input("Locations (comma-separated keys): ").strip()
            locations = (
                [loc.strip() for loc in location_input.split(",") if loc.strip()]
                if location_input
                else list(scraper.locations.keys())
            )
            max_results_input = input("Max posts [50]: ").strip()
            max_results = int(max_results_input) if max_results_input else 50
            max_results = max(1, min(max_results, 100))
            per_location_limit = None

        radius_input = input("Search radius km [15]: ").strip()
        radius_km = int(radius_input) if radius_input else 15

        minutes_input = input("Minutes back [30]: ").strip()
        minutes_back = int(minutes_input) if minutes_input else 30

        if use_all_locations:
            rows = scraper.search_recent_tweets_by_location(
                keywords=keywords,
                locations=locations,
                per_location_limit=per_location_limit or 5,
                radius_km=radius_km,
                minutes_back=minutes_back,
            )
            search_config = {
                "keywords": keywords,
                "locations": [scraper.locations[k]["name"] for k in locations if k in scraper.locations],
                "radius_km": radius_km,
                "minutes_back": minutes_back,
                "max_results": per_location_limit,
                "mode": "per_location",
            }
        else:
            query = scraper.build_geographic_query(
                keywords=keywords,
                locations=locations,
                radius_km=radius_km,
                minutes_back=minutes_back,
            )
            search_results = scraper.search_recent_tweets(query=query, max_results=max_results)
            rows = scraper.process_results(search_results, locations)
            search_config = {
                "keywords": keywords,
                "locations": locations,
                "radius_km": radius_km,
                "minutes_back": minutes_back,
                "max_results": max_results,
                "mode": "combined_query",
            }

        report = scraper.format_output(rows, search_config)
        print("\n" + report)

        if rows:
            save_option = input("\nSave results to CSV? (y/n): ").strip().lower()
            if save_option == "y":
                filename = scraper.save_results(rows)
                if filename:
                    print(f"Saved to: {filename}")

    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as exc:  # noqa: BLE001
        logger.error("Execution error: %s", exc)
        print(f"Error: {exc}")


if __name__ == "__main__":
    main()
