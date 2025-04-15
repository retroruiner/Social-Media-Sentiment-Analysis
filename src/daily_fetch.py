import json
import logging
import os
from datetime import datetime
import sys

from bluesky_manager import BlueSkyManager
from data_processor import DataProcessor

# SQLAlchemy imports from your models
from database.models import DailySentimentSummary, Session

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def run_daily_fetch(query: str):
    try:
        logging.info(f"Starting daily fetch and analysis for query: {query}")
        bs_manager = BlueSkyManager()
        bs_manager.login()
        file_path = bs_manager.get_posts(query, pages=50)
        logging.info(f"Posts fetched and stored in {file_path}")

        # Read the fetched posts
        with open(file_path, "r") as infile:
            data = json.load(infile)
        posts = data.get("posts", [])
        if not posts:
            logging.warning("No posts found. Exiting daily fetch.")
            return

        # Process the posts with DataProcessor
        processor = DataProcessor(posts)
        result = {
            "sentiment_distribution": processor.get_sentiment_distribution(),
            "aggregate_by_date": processor.aggregate_by_date().to_dict(
                orient="records"
            ),
            "sentiment_over_time": processor.aggregate_sentiment_by_date().to_dict(
                orient="records"
            ),
            "word_frequency": processor.get_word_frequency(),
            "heatmap_data": processor.get_heatmap_data(),
            "top_words_by_sentiment": processor.get_top_words_by_sentiment(),
        }
        logging.info("Data analysis complete.")

        # Store the result in the PostgreSQL database
        session = Session()
        try:
            daily_summary = DailySentimentSummary(
                query=query,
                date=datetime.now(datetime.timezone.utc),
                sentiment_distribution=result["sentiment_distribution"],
                aggregate_by_date=result["aggregate_by_date"],
                sentiment_over_time=result["sentiment_over_time"],
                word_frequency=result["word_frequency"],
                heatmap_data=result["heatmap_data"],
                top_words_by_sentiment=result["top_words_by_sentiment"],
            )
            session.add(daily_summary)
            session.commit()
            logging.info("Daily sentiment summary stored successfully in the database.")
        except Exception as db_error:
            session.rollback()
            logging.exception("Error storing the daily sentiment summary:")
        finally:
            session.close()
    except Exception as e:
        logging.exception("Error during daily fetch and analysis:")


if __name__ == "__main__":
    # You can pass a query via an environment variable or default to "Macron"
    fetch_query = os.getenv("FETCH_QUERY", "Macron")
    run_daily_fetch(fetch_query)
