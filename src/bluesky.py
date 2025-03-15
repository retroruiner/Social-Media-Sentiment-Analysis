from atproto import Client
import requests
from utils.json_manager import JsonFileManager


class BlueSkyManager:
    def __init__(self, data_folder: str = None):
        """
        Initializes the BlueSkyManager with an optional data folder for JSON storage.
        Also creates an instance of JsonFileManager.
        """
        self.client = None
        self.json_manager = JsonFileManager(data_folder)

    def get_posts(self, query: str) -> str:
        """
        Retrieves posts from the BlueSky feed based on a query, filters the data,
        and then stores the result as a JSON file using JsonFileManager.

        Args:
            query (str): The search query for posts.

        Returns:
            str: The file path of the stored JSON file.
        """
        url = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
        params = {"q": query, "limit": 20, "lang": "en"}

        response = requests.get(url, params=params)
        if response.status_code == 200:
            json_response = self.filter_posts_data(response.json())
        else:
            json_response = {
                "error": True,
                "status_code": response.status_code,
                "message": response.text,
            }

        filename = self.json_manager.generate_filename(query)
        file_path = self.json_manager.store_json(json_response, filename)
        return file_path

    def filter_posts_data(self, data: dict) -> dict:
        """
        Filters the posts data to include only the text, creation time, language, and author handle.

        Args:
            data (dict): The original JSON data containing posts.

        Returns:
            dict: A dictionary with the filtered posts.
        """
        filtered_posts = []
        for post in data.get("posts", []):
            record = post.get("record", {})
            author = post.get("author", {})
            filtered_post = {
                "text": record.get("text", ""),
                "createdAt": record.get("createdAt", ""),
                "language": record.get("langs", [None])[0],
                "author": author.get("handle", ""),
            }
            filtered_posts.append(filtered_post)
        return {"posts": filtered_posts}
