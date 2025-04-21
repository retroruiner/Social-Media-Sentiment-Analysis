import os
import requests
import asyncio
import logging
from datetime import datetime, timezone, date
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

    def get_posts(
        self, query: str, target_date: date = None, max_pages: int = 10
    ) -> dict:
        """
        Fetch posts matching `query` until we see a post older than `target_date`,
        then stop. Only returns posts created exactly on `target_date`.
        """
        if target_date is None:
            # default to “today” in UTC
            target_date = datetime.now(timezone.utc).date()

        headers = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        all_posts = []
        cursor = None
        retry = False

        for _ in range(max_pages):
            params = {"q": query, "limit": 100, "lang": "en"}
            if cursor:
                params["cursor"] = cursor

            resp = requests.get(
                "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts",
                params=params,
                headers=headers,
            )
            # handle expired token once
            if resp.status_code == 403 and not retry:
                logging.warning("Token expired, re-authenticating…")
                self.login()
                headers["Authorization"] = f"Bearer {self.access_token}"
                retry = True
                continue
            resp.raise_for_status()
            data = resp.json()

            # examine each post’s createdAt
            page_posts = data.get("posts", [])
            to_keep = []
            for p in page_posts:
                created = p["record"].get("createdAt")
                if not created:
                    continue
                created_dt = isoparse(created)
                created_d = created_dt.date()
                # if we’ve gone past the target day, stop everything
                if created_d < target_date:
                    logging.info(
                        f"Encountered post on {created_d}, which is before {target_date}; stopping."
                    )
                    page_posts = None
                    break
                # only keep if exactly on the target date
                if created_d == target_date:
                    to_keep.append(p)
            if page_posts is None:
                break

            if not to_keep:
                # no posts on target date in this page, but still more pages might have them
                cursor = data.get("cursor")
                if not cursor:
                    break
                continue

            # collect posts without translation
            for p in to_keep:
                rec = p["record"]
                all_posts.append(
                    {
                        "text": rec.get("cleaned_text") or rec.get("text", ""),
                        "createdAt": rec.get("createdAt", ""),
                        "author": p["author"].get("handle", ""),
                        "language": "en",
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
            author = p["author"].get("handle", "")

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
                    "author": author,
                    "language": language,
                }
            )
        return out
