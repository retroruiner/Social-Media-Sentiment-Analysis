import os
import requests
from utils.translate_posts import PostTranslator
from utils.json_manager import JsonFileManager
import asyncio


class BlueSkyManager:
    def __init__(self, data_folder: str = None):
        """
        Initializes the BlueSkyManager with an optional data folder for JSON storage.
        Also creates an instance of JsonFileManager.
        """
        self.client = None
        self.json_manager = JsonFileManager(data_folder)
        self.access_token = None

    def login(self):
        """
        Logs into the BlueSky account only if an access token does not already exist.
        """
        if self.access_token:
            print("Access token already exists. Skipping login.")
            return

        username = os.getenv("BLUESKY_USERNAME")
        password = os.getenv("BLUESKY_PASSWORD")

        if not username or not password:
            raise ValueError("Missing BlueSky credentials in environment variables.")

        url = "https://bsky.social/xrpc/com.atproto.server.createSession"
        response = requests.post(
            url, json={"identifier": username, "password": password}
        )
        response.raise_for_status()
        session = response.json()
        self.access_token = session["accessJwt"]
        print(f"Access JWT: {self.access_token}")

    def get_posts(self, query: str, pages: int = 5) -> str:
        """
        Retrieves posts from the BlueSky feed based on a query.
        If an existing file is found, it is used instead of fetching new data.
        Handles token expiration by retrying once after re-login.
        """
        existing_file_path = self.json_manager.get_existing_file_path(query)

        if existing_file_path:
            print(f"Using existing data file: {existing_file_path}")
            return (
                existing_file_path  # Use the existing file instead of fetching new data
            )

        url = "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"
        headers = {}

        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        all_filtered_posts = []
        cursor = None
        retry_attempted = False

        for _ in range(pages):
            params = {"q": query, "limit": 100, "lang": "en"}
            if cursor:
                params["cursor"] = cursor

            response = requests.get(url, params=params, headers=headers)

            if response.status_code == 403 and not retry_attempted:
                print("Access token expired. Re-authenticating...")
                self.login()
                headers["Authorization"] = f"Bearer {self.access_token}"
                retry_attempted = True
                continue

            if response.status_code != 200:
                print(f"Error fetching posts: {response.status_code} - {response.text}")
                break

            json_data = response.json()
            filtered_response = self.filter_and_translate_posts(json_data)
            posts = filtered_response.get("posts", [])

            if not posts:
                break  # No more posts available

            all_filtered_posts.extend(posts)
            cursor = json_data.get("cursor")

            if not cursor:
                break  # No further pages available

        final_json_response = {"posts": all_filtered_posts}
        filename = self.json_manager.generate_filename(query)
        file_path = self.json_manager.store_json(final_json_response, filename)
        return file_path

    def filter_and_translate_posts(self, data: dict) -> dict:
        """
        Filters and translates posts.
        """
        translator = PostTranslator()
        posts = data.get("posts", [])
        filtered_posts = []
        tasks = []

        for post in posts:
            record = post.get("record", {})
            text = record.get("cleaned_text", record.get("text", ""))
            tasks.append(translator.translate_text(text))

        async def run_translations():
            return await asyncio.gather(*tasks)

        translations = asyncio.run(run_translations())

        for post, translation_result in zip(posts, translations):
            record = post.get("record", {})
            author = post.get("author", {})
            filtered_post = {
                "text": translation_result["text"],
                "createdAt": record.get("createdAt", ""),
                "author": author.get("handle", ""),
                "language": translation_result["language"],
            }
            filtered_posts.append(filtered_post)

        return {"posts": filtered_posts}
