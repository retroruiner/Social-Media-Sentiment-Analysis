import praw
from utils.json_manager import JsonFileManager


class RedditManager:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        user_agent: str,
        data_folder: str = None,
    ):
        """
        Initializes the RedditManager with Reddit API credentials and an optional data folder for JSON storage.
        Creates an instance of JsonFileManager.

        Args:
            client_id (str): Your Reddit app's client ID.
            client_secret (str): Your Reddit app's secret key.
            user_agent (str): A descriptive user agent string.
            data_folder (str): Optional folder path for JSON storage.
        """
        self.reddit = praw.Reddit(
            client_id=client_id, client_secret=client_secret, user_agent=user_agent
        )
        self.json_manager = JsonFileManager(data_folder)

    def get_posts(self, query: str, limit: int = 100) -> str:
        """
        Retrieves posts from Reddit based on a query, filters relevant data,
        and stores the result as a JSON file.

        Args:
            query (str): The search query for posts.

        Returns:
            str: The file path of the stored JSON file.
        """
        subreddit = self.reddit.subreddit("all")
        search_results = subreddit.search(query=query, limit=100)

        filtered_posts = self.filter_posts_data(search_results)

        filename = self.json_manager.generate_filename(query)
        file_path = self.json_manager.store_json(filtered_posts, filename)

        return file_path

    def filter_posts_data(self, posts) -> dict:
        """
        Filters the posts data to include only relevant fields.

        Args:
            posts (iterable): Iterable of PRAW Submission objects.

        Returns:
            dict: Dictionary containing filtered post details.
        """
        filtered_posts = []

        for post in posts:
            filtered_post = {
                "title": post.title,
                "text": post.selftext,
                "createdAt": post.created_utc,
                "author": str(post.author),
                "subreddit": post.subreddit.display_name,
                "url": post.url,
                "num_comments": post.num_comments,
                "score": post.score,
            }
            filtered_posts.append(filtered_post)

        return {"posts": filtered_posts}

    def filter_posts_data(self, posts) -> dict:
        """
        Filters the Reddit posts data to include specific details.

        Args:
            posts: Iterable containing PRAW submission objects.

        Returns:
            dict: Dictionary containing filtered posts data.
        """
        filtered_posts = []

        for post in posts:
            filtered_post = {
                "title": post.title,
                "text": post.selftext,
                "createdAt": post.created_utc,
                "author": str(post.author),
                "subreddit": post.subreddit.display_name,
                "url": post.url,
                "num_comments": post.num_comments,
                "score": post.score,
            }
            filtered_posts.append(filtered_post)

        return {"posts": filtered_posts}
