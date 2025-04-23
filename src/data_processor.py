import logging
import nltk

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def ensure_nltk_resources():
    """
    Check for and download required NLTK data:
      - punkt tokenizer
      - WordNet corpus
      - Omw multilingual WordNet data
      - stopwords corpus
    """
    resources = {
        "punkt": "tokenizers/punkt",
        "wordnet": "corpora/wordnet",
        "omw-1.4": "corpora/omw-1.4",
        "stopwords": "corpora/stopwords",
    }
    for pkg, path in resources.items():
        try:
            nltk.data.find(path)
            logging.info(f"NLTK resource '{pkg}' is already installed.")
        except LookupError:
            logging.info(f"NLTK resource '{pkg}' not found — downloading…")
            nltk.download(pkg)


ensure_nltk_resources()

import pandas as pd
from collections import Counter
import datetime
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import string
from nltk.corpus import stopwords
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


class DataProcessor:
    def __init__(self, posts: list):
        """
        Initializes the DataProcessor with a list of posts.
        Each post should be a dictionary containing keys like 'text', 'createdAt', 'sentiment', and 'confidence'.
        """
        self.posts = posts
        logging.info(f"DataProcessor initialized with {len(posts)} posts.")

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
        logging.info(f"Sentiment distribution calculated: {distribution}")
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
                    logging.debug(f"Parsed date {created_at} to {date_obj}")
                except Exception as e:
                    logging.error(f"Error parsing date {created_at}: {e}")
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
            logging.info("Aggregated posts by date successfully.")
            return grouped
        else:
            logging.warning("No data available to aggregate by date.")
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
                # Let pandas infer the format; strip trailing Z if present
                ts = created_at.rstrip("Z")
                dt_obj = pd.to_datetime(ts, utc=True, errors="coerce")
                sentiment = post.get("sentiment", "UNKNOWN")
                data.append({"datetime": dt_obj, "sentiment": sentiment})
            except Exception as e:
                logging.error(f"Error parsing date '{created_at}': {e}")
                continue

        if not data:
            logging.warning("No valid datetime data available for aggregation by date.")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        unique_dates = df["datetime"].dt.date.unique()
        logging.info(f"Unique dates found: {unique_dates}")

        if len(unique_dates) == 1:
            df["hour"] = df["datetime"].dt.hour
            aggregated = (
                df.groupby(["hour", "sentiment"]).size().reset_index(name="count")
            )
            aggregated.sort_values(by="hour", inplace=True)
            logging.info("Aggregated sentiment by hour for single date.")
        else:
            df["date"] = df["datetime"].dt.date
            aggregated = (
                df.groupby(["date", "sentiment"]).size().reset_index(name="count")
            )
            aggregated.sort_values(by="date", inplace=True)
            logging.info("Aggregated sentiment by date for multiple dates.")

        return aggregated

    def get_word_frequency(
        self, min_word_length: int = 3, filter_rare: bool = True
    ) -> dict:
        """
        Returns a dictionary of word frequencies from cleaned posts.
        - Removes stopwords, punctuation, short words, and numbers.
        - Applies lemmatization.
        - Optionally filters out rare (count == 1) words.
        """

        lemmatizer = WordNetLemmatizer()
        stop_words = set(stopwords.words("english"))

        words = []

        for post in self.posts:
            text = post.get("cleaned_text", post.get("text", ""))
            tokens = word_tokenize(text)

            for token in tokens:
                token = token.lower().strip()

                # Filter unwanted tokens
                if (
                    token in stop_words
                    or token in string.punctuation
                    or len(token) < min_word_length
                    or not token.isalpha()
                ):
                    continue

                lemma = lemmatizer.lemmatize(token)
                words.append(lemma)

        frequency = Counter(words)

        if filter_rare:
            frequency = {word: count for word, count in frequency.items() if count > 1}

        logging.info(f"Calculated word frequency for {len(frequency)} words.")
        return dict(frequency)

    def get_top_words_by_sentiment(self, top_n=10, min_word_length=3) -> dict:
        """
        Returns the top N words for each sentiment category.

        Args:
            top_n (int): Number of top words to retrieve per sentiment.
            min_word_length (int): Minimum length of words to include.

        Returns:
            dict: Dictionary containing sentiments as keys and lists of top words with their counts as values.
        """

        lemmatizer = WordNetLemmatizer()
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

        sentiment_words = {}

        for sentiment in ["POSITIVE", "NEGATIVE"]:
            words = []
            for post in self.posts:
                if post.get("sentiment") == sentiment:
                    text = post.get("cleaned_text", post.get("text", "")).lower()
                    tokens = word_tokenize(text)
                    for token in tokens:
                        token = token.lower()
                        if (
                            token in string.punctuation
                            or token in custom_stopwords
                            or len(token) < min_word_length
                            or any(char.isdigit() for char in token)
                        ):
                            continue
                        lemma = lemmatizer.lemmatize(token)
                        words.append(lemma)

            frequency = Counter(words).most_common(top_n)
            sentiment_words[sentiment] = dict(frequency)

        return sentiment_words

    def analyze_text_length_sentiment(self) -> pd.DataFrame:
        """
        Analyzes the relationship between text length and sentiment.
        Returns a DataFrame with:
            - 'text_length': word count of the cleaned text
            - 'net_sentiment': scaled sentiment score in the range [-1, 1].
        """
        data = []
        for post in self.posts:
            text = post.get("cleaned_text", post.get("text", "")).strip()
            word_count = len(text.split())
            if word_count < 3:
                continue

            sentiment_label = post.get("sentiment", "").upper()
            confidence = post.get("confidence", None)
            if not sentiment_label or confidence is None:
                continue

            if sentiment_label == "POSITIVE":
                net_sentiment = 2 * (confidence - 0.5)
            elif sentiment_label == "NEGATIVE":
                net_sentiment = -2 * (confidence - 0.5)
            else:
                net_sentiment = 0

            data.append({"text_length": word_count, "net_sentiment": net_sentiment})

        df = pd.DataFrame(data)
        df.sort_values(by="text_length", inplace=True)
        df.reset_index(drop=True, inplace=True)
        logging.info("Analyzed text length and sentiment relationship.")
        return df

    def filter_by_keywords(self, keywords: list) -> list:
        """
        Returns posts that contain any of the specified keywords in their text.
        Args:
            keywords (list): A list of keywords to look for in the post text.
        """
        filtered_posts = []
        for post in self.posts:
            text = post.get("cleaned_text", post.get("text", "")).lower()
            if any(keyword.lower() in text for keyword in keywords):
                filtered_posts.append(post)
        logging.info(
            f"Filtered posts by keywords {keywords}: {len(filtered_posts)} found."
        )
        return filtered_posts

    def filter_by_sentiment(self, sentiment: str) -> list:
        """
        Returns posts that match the specified sentiment.
        Args:
            sentiment (str): The sentiment to filter by (e.g., "POSITIVE" or "NEGATIVE").
        """
        filtered = [
            post
            for post in self.posts
            if post.get("sentiment", "").upper() == sentiment.upper()
        ]
        logging.info(
            f"Filtered posts by sentiment '{sentiment}': {len(filtered)} found."
        )
        return filtered

    def get_heatmap_data(self) -> dict:
        """
        Creates a dictionary for a heatmap visualization showing post counts
        grouped by day of the week and hour of the day.
        """
        posts_df = pd.DataFrame(self.posts)
        if "createdAt" not in posts_df.columns:
            logging.warning("createdAt column not found in posts data.")
            return {}

        # let pandas infer the format, coerce errors to NaT
        posts_df["createdAt"] = pd.to_datetime(
            posts_df["createdAt"].str.rstrip("Z"), utc=True, errors="coerce"
        )
        posts_df["day_of_week"] = posts_df["createdAt"].dt.day_name()
        posts_df["hour"] = posts_df["createdAt"].dt.hour

        heatmap_data = (
            posts_df.groupby(["day_of_week", "hour"]).size().reset_index(name="count")
        )
        pivot = heatmap_data.pivot(index="day_of_week", columns="hour", values="count")
        days_order = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        pivot = pivot.reindex(days_order)
        pivot = pivot.fillna(0)
        logging.info("Generated heatmap data successfully.")
        return pivot.to_dict(orient="index")
