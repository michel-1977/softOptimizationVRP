from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
import json
import re
import urllib.error
import urllib.parse
import urllib.request


class SocialGeoScraper:
    def __init__(
        self,
        bluesky_api_base: str = "https://api.bsky.app",
        bluesky_identifier: str = "",
        bluesky_app_password: str = "",
        timeout_sec: int = 10,
    ) -> None:
        self.bluesky_api_base = bluesky_api_base.rstrip("/")
        self.bluesky_identifier = bluesky_identifier.strip()
        self.bluesky_app_password = bluesky_app_password.strip()
        self.timeout_sec = max(3, int(timeout_sec))
        self._bluesky_access_jwt = ""

    def search_recent_posts(
        self,
        location_name: str,
        keywords: str,
        per_location_limit: int = 5,
        minutes_back: int = 30,
        lang: str = "",
    ) -> List[Dict[str, Any]]:
        query = self._build_query(keywords, location_name)
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(minutes_back)))
        lang_filter = _normalize_lang_tag(lang)
        posts = self._search_bluesky(
            query=query,
            limit=per_location_limit,
            cutoff=cutoff,
            lang_filter=lang_filter,
        )
        posts.sort(key=lambda x: str(x.get("created_at", "")), reverse=True)
        return posts[: max(1, int(per_location_limit))]

    def _build_query(self, keywords: str, location_name: str) -> str:
        normalized_keywords = " ".join(str(keywords or "").split())
        normalized_location = " ".join(str(location_name or "").split())
        if normalized_keywords and normalized_location:
            return f"({normalized_keywords}) ({normalized_location})"
        return normalized_keywords or normalized_location

    def _http_get_json(
        self, url: str, headers: Dict[str, str] | None = None
    ) -> Dict[str, Any]:
        merged_headers = {
            "Accept": "application/json",
            "User-Agent": "softOptimizationVRP/1.0 (+social-scraper)",
        }
        if headers:
            merged_headers.update(headers)

        req = urllib.request.Request(url, headers=merged_headers)
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as response:
            data = response.read()
        payload = json.loads(data.decode("utf-8"))
        return payload if isinstance(payload, dict) else {}

    def _http_post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "softOptimizationVRP/1.0 (+social-scraper)",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as response:
            data = response.read()
        parsed = json.loads(data.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else {}

    def _ensure_bluesky_access_token(self) -> str:
        if self._bluesky_access_jwt:
            return self._bluesky_access_jwt
        if not self.bluesky_identifier or not self.bluesky_app_password:
            return ""

        login_url = "https://bsky.social/xrpc/com.atproto.server.createSession"
        try:
            payload = self._http_post_json(
                login_url,
                {
                    "identifier": self.bluesky_identifier,
                    "password": self.bluesky_app_password,
                },
            )
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
            return ""

        token = str(payload.get("accessJwt", "")).strip()
        if token:
            self._bluesky_access_jwt = token
        return token

    def _search_bluesky(
        self, query: str, limit: int, cutoff: datetime, lang_filter: str = ""
    ) -> List[Dict[str, Any]]:
        endpoint = f"{self.bluesky_api_base}/xrpc/app.bsky.feed.searchPosts"
        params_obj: Dict[str, Any] = {
            "q": query,
            "limit": max(1, min(int(limit), 100)),
        }
        if lang_filter:
            params_obj["lang"] = lang_filter
        params = urllib.parse.urlencode(params_obj)
        url = f"{endpoint}?{params}"

        access_token = self._ensure_bluesky_access_token()
        headers = (
            {"Authorization": f"Bearer {access_token}"}
            if access_token
            else None
        )

        try:
            payload = self._http_get_json(url, headers=headers)
        except urllib.error.HTTPError as exc:
            if exc.code in {401, 403} and access_token:
                # Token may have expired; retry once after forcing refresh.
                self._bluesky_access_jwt = ""
                retry_token = self._ensure_bluesky_access_token()
                retry_headers = (
                    {"Authorization": f"Bearer {retry_token}"}
                    if retry_token
                    else None
                )
                if retry_headers:
                    try:
                        payload = self._http_get_json(url, headers=retry_headers)
                    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
                        return []
                else:
                    return []
            else:
                return []
        except (urllib.error.URLError, TimeoutError):
            return []

        rows = []
        for item in payload.get("posts", []) or []:
            if not isinstance(item, dict):
                continue

            record = item.get("record", {}) if isinstance(item.get("record"), dict) else {}
            post_langs = _extract_post_langs(record)
            if lang_filter and not _post_matches_lang_filter(post_langs, lang_filter):
                continue

            created_raw = record.get("createdAt")
            created = _parse_utc_datetime(created_raw)
            if created and created < cutoff:
                continue

            author = item.get("author", {}) if isinstance(item.get("author"), dict) else {}
            text = record.get("text", "")
            uri = str(item.get("uri", "")).strip()
            post_id = _extract_post_id_from_uri(uri)
            handle = str(author.get("handle") or "").strip()
            did = str(author.get("did") or "").strip()
            post_url = _build_post_url(uri=uri, handle=handle, did=did)

            rows.append(
                {
                    "created_at": _value_to_serializable(created_raw),
                    "username": _value_to_serializable(handle),
                    "tweet_id": _value_to_serializable(post_id),
                    "text": _value_to_serializable(text),
                    "tweet_url": _value_to_serializable(post_url),
                    "post_uri": _value_to_serializable(uri),
                    "like_count": _value_to_serializable(item.get("likeCount")),
                    "retweet_count": _value_to_serializable(item.get("repostCount")),
                    "reply_count": _value_to_serializable(item.get("replyCount")),
                    "post_langs": post_langs,
                    "source_platform": "bluesky",
                }
            )

        return rows


def _parse_utc_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
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


def _value_to_serializable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:  # noqa: BLE001 - fallback to string
            pass
    return str(value)


def _extract_post_id_from_uri(uri: str) -> str:
    raw = str(uri or "").strip()
    if not raw or not raw.startswith("at://"):
        return ""
    # Expected shape: at://<did-or-handle>/app.bsky.feed.post/<rkey>
    parts = raw[5:].split("/")
    if len(parts) < 3:
        return ""
    return str(parts[-1]).strip()


def _normalize_lang_tag(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("_", "-")
    if not raw:
        return ""
    cleaned = re.sub(r"[^a-z0-9-]", "", raw)
    if not cleaned:
        return ""
    return cleaned


def _extract_post_langs(record: Dict[str, Any]) -> List[str]:
    langs_raw = record.get("langs")
    rows: List[str] = []
    if isinstance(langs_raw, list):
        for item in langs_raw:
            lang = _normalize_lang_tag(item)
            if lang:
                rows.append(lang)
    elif isinstance(record.get("lang"), str):
        lang = _normalize_lang_tag(record.get("lang"))
        if lang:
            rows.append(lang)

    deduped: List[str] = []
    seen = set()
    for lang in rows:
        if lang in seen:
            continue
        seen.add(lang)
        deduped.append(lang)
    return deduped


def _lang_matches(post_lang: str, lang_filter: str) -> bool:
    post_token = _normalize_lang_tag(post_lang)
    filter_token = _normalize_lang_tag(lang_filter)
    if not post_token or not filter_token:
        return False
    return post_token == filter_token or post_token.startswith(filter_token + "-")


def _post_matches_lang_filter(post_langs: List[str], lang_filter: str) -> bool:
    if not lang_filter:
        return True
    if not isinstance(post_langs, list) or not post_langs:
        return False
    return any(_lang_matches(lang, lang_filter) for lang in post_langs)


def _build_post_url(uri: str, handle: str, did: str) -> str:
    post_id = _extract_post_id_from_uri(uri)
    if not post_id:
        return ""

    actor = str(handle or "").strip() or str(did or "").strip()
    if not actor:
        return ""

    actor_encoded = urllib.parse.quote(actor, safe=":@._-")
    post_encoded = urllib.parse.quote(post_id, safe="-._~")
    return f"https://bsky.app/profile/{actor_encoded}/post/{post_encoded}"
