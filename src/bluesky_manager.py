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
        Logs into the BlueSky account using provided credentials and stores the access token.

        Args:
            username (str): The username or handle of the BlueSky account.
            password (str): The password for the BlueSky account.
        """
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
        Retrieves posts from the BlueSky feed based on a query, handles pagination,
        filters the data, and then stores the result as a JSON file using JsonFileManager.

        Args:
            query (str): The search query for posts.
            pages (int): Number of pages to fetch (default is 5).

        Returns:
            str: The file path of the stored JSON file.
        """
        url = "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"
        headers = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        all_filtered_posts = []
        cursor = None

        for _ in range(pages):
            params = {"q": query, "limit": 100, "lang": "en"}
            if cursor:
                params["cursor"] = cursor

            response = requests.get(url, params=params, headers=headers)

            if response.status_code != 200:
                print(f"Error fetching posts: {response.status_code} - {response.text}")
                break

            json_data = response.json()
            filtered_response = self.filter_and_translate_posts(json_data)
            posts = filtered_response.get("posts", [])

            if not posts:
                # No more posts available
                break

            all_filtered_posts.extend(posts)

            # Update cursor for next page
            cursor = json_data.get("cursor")

            if not cursor:
                # No further pages available
                break

        final_json_response = {"posts": all_filtered_posts}
        filename = self.json_manager.generate_filename(query)
        file_path = self.json_manager.store_json(final_json_response, filename)
        return file_path

    def filter_and_translate_posts(self, data: dict) -> dict:
        """
        Filters the posts data to include only the cleaned text (or fallback to original text), creation time, and author handle.
        For each post, uses PostTranslator to detect and translate non-English posts.
        Posts that needed translation will have their language set to 'machine-en'; otherwise 'en'.

        Args:
            data (dict): The original JSON data containing posts.

        Returns:
            dict: A dictionary with the filtered and translated posts.
        """
        translator = PostTranslator()
        posts = data.get("posts", [])
        filtered_posts = []
        tasks = []

        # Create a list of translation tasks using cleaned_text when available
        for post in posts:
            record = post.get("record", {})
            # Use cleaned_text if present, otherwise fallback to the original text
            text = record.get("cleaned_text", record.get("text", ""))
            tasks.append(translator.translate_text(text))

        async def run_translations():
            return await asyncio.gather(*tasks)

        # Run all translation tasks concurrently
        translations = asyncio.run(run_translations())

        # Reconstruct posts with the translation results
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
