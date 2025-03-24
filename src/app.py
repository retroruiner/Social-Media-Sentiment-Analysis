from flask import Flask, jsonify, request
import json
import logging
import pandas as pd
from bluesky_manager import BlueSkyManager
from utils.text_cleaner import TextCleaner
from sentiment_analyzer import SentimentAnalyzer
from data_processor import DataProcessor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

app = Flask(__name__)


@app.route("/fetch_posts", methods=["GET"])
def fetch_posts():
    """Fetch posts from BlueSky and analyze sentiment"""
    query = request.args.get("query", "Macron")  # Default query is "Macron"
    logging.info(f"Received fetch_posts request with query: {query}")
    bs_manager = BlueSkyManager()

    try:
        bs_manager.login()

        file_path = bs_manager.get_posts(query, pages=50)

        with open(file_path, "r") as infile:
            data = json.load(infile)
        logging.info("JSON data loaded from file.")

        posts = data.get("posts", [])
        if not posts:
            logging.warning("No posts found in the fetched data.")
            return jsonify({"message": "No posts found"}), 404

        cleaner = TextCleaner()
        analyzer = SentimentAnalyzer()

        logging.info("Cleaning post texts.")
        cleaned_texts = [cleaner.clean_text(post["text"]) for post in posts]
        logging.info(f"Cleaned {len(cleaned_texts)} texts.")

        logging.info("Performing sentiment analysis.")
        sentiment_results = analyzer.analyze_texts(cleaned_texts)
        logging.info("Sentiment analysis completed.")

        for post, sentiment, clean in zip(posts, sentiment_results, cleaned_texts):
            post["sentiment"] = sentiment["label"]
            post["confidence"] = sentiment["score"]
            post["cleaned_text"] = clean

        logging.info("Post processing complete, returning posts data.")
        return jsonify(posts)

    except Exception as e:
        logging.exception("Error in fetch_posts endpoint:")
        return jsonify({"error": str(e)}), 500


@app.route("/analyze_data", methods=["POST"])
def analyze_data():
    posts = request.json.get("posts", [])
    logging.info(f"Received analyze_data request with {len(posts)} posts.")

    if not posts:
        logging.warning("No posts provided in the request.")
        return jsonify({"error": "No posts provided"}), 400

    try:
        processor = DataProcessor(posts)
        logging.info("Processing data using DataProcessor.")

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
        logging.info("Data analysis complete, returning results.")
        return jsonify(result)
    except Exception as e:
        logging.exception("Error in analyze_data endpoint:")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
