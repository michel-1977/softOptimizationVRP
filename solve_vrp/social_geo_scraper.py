from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
import json
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
    ) -> List[Dict[str, Any]]:
        query = self._build_query(keywords, location_name)
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(minutes_back)))
        posts = self._search_bluesky(
            query=query,
            limit=per_location_limit,
            cutoff=cutoff,
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
        self, query: str, limit: int, cutoff: datetime
    ) -> List[Dict[str, Any]]:
        endpoint = f"{self.bluesky_api_base}/xrpc/app.bsky.feed.searchPosts"
        params = urllib.parse.urlencode(
            {"q": query, "limit": max(1, min(int(limit), 100))}
        )
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

            created_raw = item.get("record", {}).get("createdAt")
            created = _parse_utc_datetime(created_raw)
            if created and created < cutoff:
                continue

            author = item.get("author", {}) if isinstance(item.get("author"), dict) else {}
            text = item.get("record", {}).get("text", "")
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
