import os
import requests
import asyncio
import logging
from datetime import datetime, timedelta, timezone, date
from dateutil.parser import isoparse
from utils.translate_posts import PostTranslator
from utils.json_manager import JsonFileManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
for noisy in ["httpx", "httpcore", "urllib3", "googletrans", "asyncio"]:
    logging.getLogger(noisy).setLevel(logging.WARNING)


class BlueSkyManager:
    def __init__(self, data_folder: str = None):
        self.translator = PostTranslator()
        self.client = None
        self.access_token = None
        logging.info("BlueSkyManager initialized.")

    def login(self):
        if self.access_token:
            logging.info("Access token exists, skipping login.")
            return
        username = os.getenv("BLUESKY_USERNAME")
        password = os.getenv("BLUESKY_PASSWORD")
        if not username or not password:
            raise ValueError("Missing BlueSky credentials.")
        url = "https://bsky.social/xrpc/com.atproto.server.createSession"
        resp = requests.post(url, json={"identifier": username, "password": password})
        resp.raise_for_status()
        self.access_token = resp.json()["accessJwt"]
        logging.info("Logged in to BlueSky.")

    def get_posts(self, query: str, target_date: date = None, limit: int = 100):
        if target_date is None:
            target_date = datetime.now(timezone.utc).date()

        # build ISO timestamps as before…
        since_ts = target_date.isoformat() + "T00:00:00.000Z"
        until_ts = (target_date + timedelta(days=1)).isoformat() + "T00:00:00.000Z"

        headers = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        all_posts = []
        cursor = None
        base_url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"

        while True:
            params = {
                "q": query,
                "lang": "en",  # server‑side filter
                "since": since_ts,
                "until": until_ts,
                "sort": "latest",
                "limit": limit,
            }
            if cursor:
                params["cursor"] = cursor

            resp = requests.get(base_url, params=params, headers=headers)
            if resp.status_code == 403:
                self.login()
                headers["Authorization"] = f"Bearer {self.access_token}"
                resp = requests.get(base_url, params=params, headers=headers)
            resp.raise_for_status()

            data = resp.json()
            posts = data.get("posts", [])
            if not posts:
                break  # no more pages (or no posts today)

            for item in posts:
                rec = item.get("record", {})
                # ---- server told us "lang=en", but double-check via rec["langs"]:
                if "langs" in rec and "en" not in rec["langs"]:
                    continue

                # ---- date filter
                created_at_str = rec.get("createdAt")
                if not created_at_str:
                    continue
                created_dt = isoparse(created_at_str).astimezone(timezone.utc)
                if created_dt.date() != target_date:
                    continue

                # ---- extract text
                text = rec.get("cleaned_text") or rec.get("text", "")
                if not text.strip():
                    continue

                all_posts.append(
                    {
                        "uri": item.get("uri"),
                        "text": text,
                        "createdAt": created_at_str,
                    }
                )

            cursor = data.get("cursor")
            if not cursor:
                break

        return {"posts": all_posts}

    def _translate_posts(self, posts: list) -> list:
        """
        Given a list of raw post dicts, run translation asynchronously using
        the already-initialized PostTranslator and return filtered dicts
        with only the relevant fields.
        """
        # Create translation tasks
        tasks = [
            self.translator.translate_text(
                p["record"].get("cleaned_text") or p["record"].get("text", "")
            )
            for p in posts
        ]

        async def runner():
            return await asyncio.gather(*tasks, return_exceptions=True)

        try:
            results = asyncio.run(runner())
        except Exception as e:
            logging.error("Translation error during batch run: %s", e)
            results = [{"text": "", "language": ""}] * len(posts)

        out = []
        for p, tr in zip(posts, results):
            rec = p["record"]

            # Handle translation exceptions individually
            if isinstance(tr, Exception):
                logging.error("Translation failed for post: %s", tr)
                translated_text = ""
                language = "error"
            else:
                translated_text = tr.get("text", "")
                language = tr.get("language", "")

            out.append(
                {
                    "text": translated_text,
                    "createdAt": rec.get("createdAt", ""),
                    "language": language,
                }
            )
        return out

    def get_posts_no_date_filter(self, query: str, max_pages: int = 10) -> dict:
        """
        Fetch posts matching `query` up to `max_pages`, without any date filtering.
        [TEMPORARY VERSION FOR TESTING]
        """
        if not self.access_token:
            logging.info(
                "No access token found, attempting login before getting posts."
            )
            self.login()
        if not self.access_token:
            raise RuntimeError("Cannot get posts without being logged in.")

        headers = {"Authorization": f"Bearer {self.access_token}"}

        all_posts = []
        cursor = None
        retry = False

        logging.info(
            f"Starting post fetch for query='{query}' (NO DATE FILTER), max_pages={max_pages}"
        )

        for page_num in range(max_pages):
            params = {"q": query, "limit": 100}
            if cursor:
                params["cursor"] = cursor

            logging.info(f"Fetching page {page_num + 1} with cursor: {cursor}")
            try:
                resp = requests.get(
                    "https://bsky.social/xrpc/app.bsky.feed.searchPosts",
                    params=params,
                    headers=headers,
                    timeout=20,
                )

                # Handle expired token (same logic as before)
                if (
                    resp.status_code == 401
                    and "ExpiredToken" in resp.text
                    and not retry
                ):
                    logging.warning("Access token expired, re-authenticating...")
                    self.login()
                    headers["Authorization"] = f"Bearer {self.access_token}"
                    retry = True
                    # Re-request immediately after successful re-login
                    resp = requests.get(
                        "https://bsky.social/xrpc/app.bsky.feed.searchPosts",
                        params=params,
                        headers=headers,
                        timeout=20,
                    )
                    logging.info(f"Retry request status: {resp.status_code}")

                resp.raise_for_status()
                data = resp.json()

            except requests.exceptions.RequestException as e:
                logging.error(f"HTTP request failed for page {page_num + 1}: {e}")
                break  # Stop fetching if a page fails

            page_posts = data.get("posts", [])
            if not page_posts:
                logging.info(f"No posts found on page {page_num + 1}.")
                cursor = data.get("cursor")
                if not cursor:
                    logging.info("No more pages (no cursor).")
                    break
                continue  # Go to next page if cursor exists

            logging.info(
                f"Processing {len(page_posts)} posts from page {page_num + 1} (adding all)."
            )

            # --- DATE FILTERING LOGIC REMOVED ---
            # No date checking, just collect all posts from the page
            for p in page_posts:
                record = p.get("record")
                if not record:
                    logging.warning(f"Post missing 'record' field: {p.get('uri')}")
                    continue
                created_at_str = record.get("createdAt")
                post_text = record.get("text", "")

                all_posts.append(
                    {
                        "uri": p.get("uri"),
                        "text": post_text,
                        "createdAt": created_at_str
                        or "",  # Keep createdAt if available
                        # "language": "en", # You might add this back if needed
                    }
                )
            # --- END OF REMOVED LOGIC ---

            # Get cursor for the next page
            cursor = data.get("cursor")
            if not cursor:
                logging.info("Reached end of results (no cursor returned by API).")
                break  # No more pages

        logging.info(
            f"Finished fetching (no date filter). Total posts collected: {len(all_posts)}"
        )
        return {"posts": all_posts}
