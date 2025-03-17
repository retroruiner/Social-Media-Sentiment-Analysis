import pandas as pd
from collections import Counter
import datetime
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import string
import nltk


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
        If there is only one unique date, aggregates by hour instead.
        Returns a DataFrame with the count of each sentiment per date (or per hour if only one date exists).

        Expected date format in the JSON is ISO (e.g., "2025-03-17T21:03:40.220Z").
        """
        data = []
        for post in self.posts:
            created_at = post.get("createdAt")
            if not created_at:
                continue

            try:
                # Parse the full datetime using pandas (handle the "Z" by replacing with "+00:00")
                dt_obj = pd.to_datetime(
                    created_at.replace("Z", "+00:00"), utc=True, errors="coerce"
                )
                if pd.isnull(dt_obj):
                    continue
            except Exception as e:
                print(f"Error parsing date '{created_at}': {e}")
                continue

            sentiment = post.get("sentiment", "UNKNOWN")
            data.append({"datetime": dt_obj, "sentiment": sentiment})

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Check the number of unique dates (without time)
        unique_dates = df["datetime"].dt.date.unique()

        if len(unique_dates) == 1:
            # Only one unique date available; aggregate by hour for finer granularity.
            df["hour"] = df["datetime"].dt.hour
            aggregated = (
                df.groupby(["hour", "sentiment"]).size().reset_index(name="count")
            )
            aggregated.sort_values(by="hour", inplace=True)
        else:
            # Multiple dates exist; aggregate by date.
            df["date"] = df["datetime"].dt.date
            aggregated = (
                df.groupby(["date", "sentiment"]).size().reset_index(name="count")
            )
            aggregated.sort_values(by="date", inplace=True)

        return aggregated

    def get_word_frequency(
        self, min_word_length: int = 3, filter_rare: bool = True
    ) -> dict:
        """
        Returns a dictionary of word frequencies from cleaned posts.
        - Filters out short, rare, or unimportant words.
        - Uses a custom stopword list.
        """
        from nltk.corpus import stopwords

        lemmatizer = WordNetLemmatizer()

        # Extended stopwords list (beyond NLTK)
        custom_stopwords = set(stopwords.words("english")).union(
            {
                "could",
                "would",
                "also",
                "first",
                "one",
                "two",
                "new",
                "old",
                "said",
                "like",
                "make",
                "thing",
                "time",
                "see",
                "get",
                "many",
                "well",
                "back",
                "year",
                "years",
            }
        )

        words = []

        for post in self.posts:
            text = post.get("cleaned_text", post.get("text", ""))
            tokens = word_tokenize(text)  # Tokenize text

            for token in tokens:
                token = token.lower()

                # Ignore punctuation, stopwords, very short words, and words with numbers
                if (
                    token in string.punctuation
                    or token in custom_stopwords
                    or len(token) < min_word_length
                    or any(char.isdigit() for char in token)
                ):
                    continue

                # Lemmatize token (normalize word forms)
                lemma = lemmatizer.lemmatize(token)
                words.append(lemma)

        # Get word frequencies
        frequency = Counter(words)

        # Remove rare words (occur only once)
        if filter_rare:
            frequency = {word: count for word, count in frequency.items() if count > 1}

        return dict(frequency)

    def analyze_text_length_sentiment(self) -> pd.DataFrame:
        """
        Analyzes the relationship between text length and sentiment.
        Returns a DataFrame with:
            - 'text_length': word count of the cleaned text
            - 'net_sentiment': scaled sentiment score in the range [-1, 1].
            0.5 becomes the neutral midpoint; 1 or -1 is extreme sentiment.
        """
        data = []
        for post in self.posts:
            # Use cleaned_text if available, otherwise fallback to raw text
            text = post.get("cleaned_text", post.get("text", "")).strip()
            word_count = len(text.split())

            # (Optional) Skip very short texts to reduce noise
            if word_count < 3:
                continue

            sentiment_label = post.get("sentiment", "").upper()
            confidence = post.get("confidence", None)

            # Skip if missing sentiment or confidence
            if not sentiment_label or confidence is None:
                continue

            # Symmetrical scaling around 0.5
            if sentiment_label == "POSITIVE":
                net_sentiment = 2 * (confidence - 0.5)
            elif sentiment_label == "NEGATIVE":
                net_sentiment = -2 * (confidence - 0.5)
            else:
                # If somehow we get an unexpected label, default to 0
                net_sentiment = 0

            data.append({"text_length": word_count, "net_sentiment": net_sentiment})

        df = pd.DataFrame(data)
        # Sorting makes it easier to interpret if you visualize
        df.sort_values(by="text_length", inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

    def filter_by_keywords(self, keywords: list) -> list:
        """
        Returns posts that contain any of the specified keywords in their text.
        Args:
            keywords (list): A list of keywords to look for in the post text.
        """
        filtered_posts = []
        for post in self.posts:
            # Use cleaned_text if available, otherwise fallback to raw text
            text = post.get("cleaned_text", post.get("text", "")).lower()
            if any(keyword.lower() in text for keyword in keywords):
                filtered_posts.append(post)
        return filtered_posts

    def filter_by_sentiment(self, sentiment: str) -> list:
        """
        Returns posts that match the specified sentiment.
        Args:
            sentiment (str): The sentiment to filter by (e.g., "POSITIVE" or "NEGATIVE").
        """
        return [
            post
            for post in self.posts
            if post.get("sentiment", "").upper() == sentiment.upper()
        ]
