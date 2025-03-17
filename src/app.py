from flask import Flask, jsonify, request
import json
import pandas as pd
from bluesky_manager import BlueSkyManager
from utils.text_cleaner import TextCleaner
from sentiment_analyzer import SentimentAnalyzer
from data_processor import DataProcessor

app = Flask(__name__)


@app.route("/fetch_posts", methods=["GET"])
def fetch_posts():
    """Fetch posts from BlueSky and analyze sentiment"""
    query = request.args.get("query", "Macron")  # Default query is "Macron"
    bs_manager = BlueSkyManager()

    try:
        bs_manager.login()
        file_path = bs_manager.get_posts(query, pages=50)

        with open(file_path, "r") as infile:
            data = json.load(infile)

        posts = data.get("posts", [])
        if not posts:
            return jsonify({"message": "No posts found"}), 404

        cleaner = TextCleaner()
        analyzer = SentimentAnalyzer()

        cleaned_texts = [cleaner.clean_text(post["text"]) for post in posts]
        sentiment_results = analyzer.analyze_texts(cleaned_texts)

        for post, sentiment, clean in zip(posts, sentiment_results, cleaned_texts):
            post["sentiment"] = sentiment["label"]
            post["confidence"] = sentiment["score"]
            post["cleaned_text"] = clean

        return jsonify(posts)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/analyze_data", methods=["POST"])
def analyze_data():
    """Perform sentiment distribution, aggregation, and word frequency analysis"""
    posts = request.json.get("posts", [])

    if not posts:
        return jsonify({"error": "No posts provided"}), 400

    processor = DataProcessor(posts)

    result = {
        "sentiment_distribution": processor.get_sentiment_distribution(),
        "aggregate_by_date": processor.aggregate_by_date().to_dict(orient="records"),
        "sentiment_over_time": processor.aggregate_sentiment_by_date().to_dict(
            orient="records"
        ),
        "word_frequency": processor.get_word_frequency(),
        "text_length_sentiment": processor.analyze_text_length_sentiment().to_dict(
            orient="records"
        ),
    }

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)
