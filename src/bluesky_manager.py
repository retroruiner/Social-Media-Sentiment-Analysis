import os
import requests
import asyncio
import logging
from datetime import datetime, timezone
from utils.translate_posts import PostTranslator
from utils.json_manager import JsonFileManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Reduce logging noise from dependencies
for noisy_lib in ["httpx", "httpcore", "urllib3", "googletrans", "asyncio"]:
    logging.getLogger(noisy_lib).setLevel(logging.WARNING)


class BlueSkyManager:
    def __init__(self, data_folder: str = None):
        """
        Initializes the BlueSkyManager with an optional data folder for JSON storage.
        Also creates an instance of JsonFileManager.
        """
        self.client = None
        self.json_manager = JsonFileManager(data_folder)
        self.access_token = None
        logging.info("BlueSkyManager initialized.")

    def login(self):
        """
        Logs into the BlueSky account only if an access token does not already exist.
        """
        if self.access_token:
            logging.info("Access token already exists. Skipping login.")
            return

        username = os.getenv("BLUESKY_USERNAME")
        password = os.getenv("BLUESKY_PASSWORD")

        if not username or not password:
            error_msg = "Missing BlueSky credentials in environment variables."
            logging.error(error_msg)
            raise ValueError(error_msg)

        url = "https://bsky.social/xrpc/com.atproto.server.createSession"
        logging.info("Attempting to log in to BlueSky.")
        response = requests.post(
            url, json={"identifier": username, "password": password}
        )
        response.raise_for_status()
        session = response.json()
        self.access_token = session["accessJwt"]
        logging.info("Logged in successfully. Access JWT obtained.")

    def get_posts(self, query: str) -> str:
        """
        Retrieves posts from the BlueSky feed based on a query.
        Only posts from the current day are kept. Fetches pages until an older post is encountered.
        If an existing file is found, it is used instead of fetching new data.
        Handles token expiration by retrying once after re-login.
        """
        self.json_manager.cleanup_old_files(keep_days=1)

        # Check for an existing file to avoid redundant fetching
        existing_file_path = self.json_manager.get_existing_file_path(query)
        if existing_file_path:
            logging.info(f"Using existing data file: {existing_file_path}")
            return existing_file_path

        url = "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"
        headers = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        all_filtered_posts = []
        cursor = None
        retry_attempted = False

        # Determine the current date (in UTC)
        current_date = datetime.now(timezone.utc).date()
        logging.info(
            f"Fetching posts for query '{query}' (only for {current_date}) until older posts are found."
        )

        page = 0
        while True:
            page += 1
            params = {"q": query, "limit": 100, "lang": "en"}
            if cursor:
                params["cursor"] = cursor

            response = requests.get(url, params=params, headers=headers)
            logging.info(
                f"Page {page}: Received response with status {response.status_code}"
            )

            # If token expired, re-login once
            if response.status_code == 403 and not retry_attempted:
                logging.warning("Access token expired. Re-authenticating...")
                self.login()
                headers["Authorization"] = f"Bearer {self.access_token}"
                retry_attempted = True
                continue

            if response.status_code != 200:
                logging.error(
                    f"Error fetching posts: {response.status_code} - {response.text}"
                )
                break

            json_data = response.json()
            filtered_response = self.filter_and_translate_posts(json_data)
            posts = filtered_response.get("posts", [])
            logging.info(f"Page {page}: Retrieved {len(posts)} posts after filtering.")

            current_day_posts = []
            for post in posts:
                created_str = post.get("createdAt", "")
                if created_str.endswith("Z"):
                    created_dt = datetime.fromisoformat(
                        created_str.replace("Z", "+00:00")
                    )
                else:
                    created_dt = datetime.fromisoformat(created_str)

                if created_dt.date() == current_date:
                    current_day_posts.append(post)
                else:
                    logging.info(
                        f"Page {page}: Found post from {created_dt.date()}, stopping fetch."
                    )
                    stop_fetching = True
                    break

            all_filtered_posts.extend(current_day_posts)
            if "stop_fetching" in locals() and stop_fetching:
                break

            cursor = json_data.get("cursor")
            if not cursor:
                logging.info(f"No more pages after page {page}.")
                break

        final_json_response = {"posts": all_filtered_posts}
        filename = self.json_manager.generate_filename(query)
        file_path = self.json_manager.store_json(final_json_response, filename)
        logging.info(f"Stored posts to file: {file_path}")
        return file_path

    def filter_and_translate_posts(self, data: dict) -> dict:
        """
        Filters and translates posts.
        """
        translator = PostTranslator()
        posts = data.get("posts", [])
        filtered_posts = []
        tasks = []

        logging.info(f"Translating {len(posts)} posts.")
        for post in posts:
            record = post.get("record", {})
            text = record.get("cleaned_text", record.get("text", ""))
            tasks.append(translator.translate_text(text))

        async def run_translations():
            return await asyncio.gather(*tasks)

        try:
            translations = asyncio.run(run_translations())
        except Exception as e:
            logging.error(f"Error during translations: {e}")
            translations = [""] * len(tasks)

        for post, translation_result in zip(posts, translations):
            record = post.get("record", {})
            author = post.get("author", {})
            filtered_post = {
                "text": translation_result.get("text", ""),
                "createdAt": record.get("createdAt", ""),
                "author": author.get("handle", ""),
                "language": translation_result.get("language", ""),
            }
            filtered_posts.append(filtered_post)

        logging.info("Filtering and translation complete.")
        return {"posts": filtered_posts}
