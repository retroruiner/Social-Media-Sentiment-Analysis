import pandas as pd
from collections import Counter
import datetime
from nltk.corpus import stopwords


class DataProcessor:
    def __init__(self, posts: list):
        """
        Initializes the DataProcessor with a list of posts.
        Each post should be a dictionary containing keys like 'text', 'createdAt', 'sentiment', and 'confidence'.
        """
        self.posts = posts

    def get_sentiment_distribution(self) -> dict:
        """
        Returns a dictionary with sentiment distribution counts.
        Example:
            {"POSITIVE": 10, "NEGATIVE": 5}
        """
        distribution = {}
        for post in self.posts:
            sentiment = post.get("sentiment", "UNKNOWN")
            distribution[sentiment] = distribution.get(sentiment, 0) + 1
        return distribution

    def aggregate_by_date(self) -> pd.DataFrame:
        """
        Aggregates posts by date based on the 'createdAt' field.
        Returns a DataFrame with the number of posts per date and average sentiment confidence.
        """
        data = []
        for post in self.posts:
            created_at = post.get("createdAt", None)
            sentiment = post.get("sentiment", "UNKNOWN")
            confidence = post.get("confidence", None)
            if created_at:
                try:
                    date_obj = datetime.datetime.fromisoformat(
                        created_at.replace("Z", "+00:00")
                    ).date()
                except Exception:
                    date_obj = None
                data.append(
                    {"date": date_obj, "sentiment": sentiment, "confidence": confidence}
                )

        df = pd.DataFrame(data)
        if not df.empty:
            grouped = (
                df.groupby("date")
                .agg(
                    {
                        "sentiment": "count",  # Count posts per date
                        "confidence": "mean",  # Average confidence per date
                    }
                )
                .rename(
                    columns={"sentiment": "post_count", "confidence": "avg_confidence"}
                )
            )
            grouped = grouped.reset_index()
            return grouped
        else:
            return pd.DataFrame()

    def aggregate_sentiment_by_date(self) -> pd.DataFrame:
        """
        Aggregates posts by date and sentiment.
        Returns a DataFrame with the count of each sentiment per date.
        """
        data = []
        for post in self.posts:
            created_at = post.get("createdAt", None)
            sentiment = post.get("sentiment", "UNKNOWN")
            if created_at:
                try:
                    date_obj = datetime.datetime.fromisoformat(
                        created_at.replace("Z", "+00:00")
                    ).date()
                except Exception:
                    date_obj = None
                data.append({"date": date_obj, "sentiment": sentiment})

        df = pd.DataFrame(data)
        if not df.empty:
            sentiment_time = (
                df.groupby(["date", "sentiment"]).size().reset_index(name="count")
            )
            return sentiment_time
        else:
            return pd.DataFrame()

    def get_word_frequency(
        self, min_word_length: int = 3, filter_rare: bool = True
    ) -> dict:
        """
        Returns a dictionary of word frequencies from the posts' cleaned text.
        Only words with length >= min_word_length and not in the stopwords list are considered.
        Optionally filters out words that occur only once if filter_rare is True.
        """
        stop_words = set(stopwords.words("english"))
        words = []
        for post in self.posts:
            # Use cleaned_text if present, otherwise use text
            text = post.get("cleaned_text", post.get("text", ""))
            tokens = text.split()  # Basic tokenization
            words.extend(
                [
                    word.lower()
                    for word in tokens
                    if len(word) >= min_word_length and word.lower() not in stop_words
                ]
            )
        frequency = Counter(words)
        if filter_rare:
            frequency = {word: count for word, count in frequency.items() if count > 1}
        return dict(frequency)
