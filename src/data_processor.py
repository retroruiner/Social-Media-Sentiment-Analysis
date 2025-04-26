import logging
import nltk

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def ensure_nltk_resources():
    """
    Check for and download required NLTK data:
      - punkt_tab tokenizer
      - WordNet corpus
      - Omw multilingual WordNet data
      - stopwords corpus
    """
    resources = {
        "punkt_tab": "tokenizers/punkt_tab",
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
            nltk.download(pkg, quiet=True)


ensure_nltk_resources()

import pandas as pd
from collections import Counter
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import string


class DataProcessor:
    def __init__(self, posts: list):
        """
        Initializes the DataProcessor with a list of posts.
        Each post should be a dictionary containing keys like 'text', 'createdAt', 'sentiment', and 'confidence'.
        """
        self.posts = posts or []
        # Initialize heavy NLP objects once
        self.lemmatizer = WordNetLemmatizer()
        self.stop_words = set(stopwords.words("english"))
        self.custom_stopwords = self.stop_words.union(
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
        logging.info(f"DataProcessor initialized with {len(self.posts)} posts.")

    def get_sentiment_distribution(self) -> dict:
        if not self.posts:
            return {}
        distribution = {}
        for post in self.posts:
            sentiment = post.get("sentiment", "UNKNOWN")
            distribution[sentiment] = distribution.get(sentiment, 0) + 1
        logging.info(f"Sentiment distribution calculated: {distribution}")
        return distribution

    def aggregate_sentiment_by_date(self) -> pd.DataFrame:
        if not self.posts:
            return pd.DataFrame()

        # Build DataFrame once
        df = (
            pd.DataFrame(self.posts)
            .assign(
                datetime=lambda d: pd.to_datetime(
                    d["createdAt"].str.rstrip("Z"), utc=True, errors="coerce"
                ),
                sentiment=lambda d: d.get("sentiment", "UNKNOWN"),
            )
            .dropna(subset=["datetime"])
        )

        # Decide grouping by hour or date
        if df["datetime"].dt.date.nunique() == 1:
            result = (
                df.assign(hour=df["datetime"].dt.hour)
                .groupby(["hour", "sentiment"])
                .size()
                .reset_index(name="count")
                .sort_values(by="hour")
            )
            logging.info("Aggregated sentiment by hour for single date.")
        else:
            result = (
                df.assign(date=df["datetime"].dt.date)
                .groupby(["date", "sentiment"])
                .size()
                .reset_index(name="count")
                .sort_values(by="date")
            )
            logging.info("Aggregated sentiment by date for multiple dates.")

        return result

    def get_word_frequency(
        self, min_word_length: int = 3, filter_rare: bool = True
    ) -> dict:
        if not self.posts:
            return {}

        counter = Counter()
        for post in self.posts:
            text = post.get("cleaned_text", post.get("text", ""))
            for token in word_tokenize(text):
                token = token.lower().strip()
                if (
                    token in self.stop_words
                    or token in string.punctuation
                    or len(token) < min_word_length
                    or not token.isalpha()
                ):
                    continue
                lemma = self.lemmatizer.lemmatize(token)
                counter[lemma] += 1

        if filter_rare:
            counter = Counter({w: c for w, c in counter.items() if c > 1})

        logging.info(f"Calculated word frequency for {len(counter)} words.")
        return dict(counter)

    def get_top_words_by_sentiment(self, top_n=10, min_word_length=3) -> dict:
        if not self.posts:
            return {"POSITIVE": {}, "NEGATIVE": {}}

        pos_counter = Counter()
        neg_counter = Counter()

        for post in self.posts:
            sentiment = post.get("sentiment", "").upper()
            text = post.get("cleaned_text", post.get("text", "")).lower()
            for token in word_tokenize(text):
                token = token.lower().strip()
                if (
                    token in self.custom_stopwords
                    or token in string.punctuation
                    or len(token) < min_word_length
                    or any(char.isdigit() for char in token)
                ):
                    continue
                lemma = self.lemmatizer.lemmatize(token)
                if sentiment == "POSITIVE":
                    pos_counter[lemma] += 1
                elif sentiment == "NEGATIVE":
                    neg_counter[lemma] += 1

        return {
            "POSITIVE": dict(pos_counter.most_common(top_n)),
            "NEGATIVE": dict(neg_counter.most_common(top_n)),
        }

    def get_heatmap_data(self) -> dict:
        if not self.posts:
            return {}

        posts_df = pd.DataFrame(self.posts)
        if "createdAt" not in posts_df.columns:
            logging.warning("createdAt column not found in posts data.")
            return {}

        posts_df["createdAt"] = pd.to_datetime(
            posts_df["createdAt"].str.rstrip("Z"), utc=True, errors="coerce"
        )
        posts_df["day_of_week"] = posts_df["createdAt"].dt.day_name()
        posts_df["hour"] = posts_df["createdAt"].dt.hour

        pivot = (
            posts_df.groupby(["day_of_week", "hour"])
            .size()
            .unstack(fill_value=0)
            .reindex(
                [
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday",
                    "Sunday",
                ],
                fill_value=0,
            )
        )
        logging.info("Generated heatmap data successfully.")
        return pivot.to_dict(orient="index")
