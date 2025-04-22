import datetime
from datetime import timezone
from flask import Flask, jsonify, request
import logging
from sqlalchemy.exc import IntegrityError

from bluesky_manager import BlueSkyManager
from utils.text_cleaner import TextCleaner
from sentiment_analyzer import SentimentAnalyzer
from data_processor import DataProcessor
from db import Session
from models import Post

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

app = Flask(__name__)


@app.route("/fetch_posts", methods=["GET"])
def fetch_posts():
    """Fetch posts from BlueSky, analyze sentiment, and store unique posts by CID."""
    query = request.args.get("query", "Macron")
    logging.info(f"Received fetch_posts request with query: {query}")
    bs_manager = BlueSkyManager()

    try:
        bs_manager.login()
        data = bs_manager.get_posts(query)
        # data = bs_manager.get_posts_no_date_filter(query, max_pages=100)
        logging.info("Data retrieved from BlueSkyManager.")

        posts = data.get("posts", [])
        if not posts:
            logging.warning(
                "No posts found in the fetched data; skipping sentiment/DB."
            )
            return "", 204

        cleaner = TextCleaner()
        analyzer = SentimentAnalyzer()

        logging.info("Cleaning post texts.")
        cleaned_texts = [cleaner.clean_text(post.get("text", "")) for post in posts]
        logging.info(f"Cleaned {len(cleaned_texts)} texts.")

        logging.info("Performing sentiment analysis.")
        sentiment_results = analyzer.analyze_texts(cleaned_texts)
        logging.info("Sentiment analysis completed.")

        session = Session()

        for post, sentiment, clean in zip(posts, sentiment_results, cleaned_texts):
            cid = post.get("cid")
            post["sentiment"] = sentiment.get("label")
            post["confidence"] = sentiment.get("score")
            post["cleaned_text"] = clean

            # parse createdAt
            created_at = None
            created_at_str = post.get("createdAt")

            if created_at_str:
                try:
                    created_at = datetime.datetime.fromisoformat(
                        created_at_str.replace("Z", "+00:00")
                    )
                except ValueError:
                    logging.warning(f"Invalid createdAt format: {created_at_str}")

            # create DB object
            db_post = Post(
                cid=cid,
                text=clean,
                sentiment=post["sentiment"],
                confidence=post["confidence"],
                created_at=created_at,
            )

            session.add(db_post)
            try:
                session.commit()
                logging.debug(f"Inserted post cid={cid}")
            except IntegrityError:
                session.rollback()
                # logging.info(f"Skipped duplicate post cid={cid}")

        session.close()

        logging.info("Post processing and saving to DB complete.")
        return jsonify(posts)

    except Exception as e:
        logging.exception("Error in fetch_posts endpoint:")
        return jsonify({"error": str(e)}), 500


@app.route("/analyze_data", methods=["POST"])
def analyze_data():
    """Fetch all stored posts and run DataProcessor on them."""
    session = Session()

    try:
        logging.info("Fetching all posts from database for analysis.")
        db_posts = session.query(Post).all()

        if not db_posts:
            logging.warning("No posts found in the database.")
            return jsonify({"error": "No posts found in database"}), 404

        posts = [
            {
                "cid": post.cid,
                "text": post.text,
                "sentiment": post.sentiment,
                "confidence": post.confidence,
                "createdAt": post.created_at.isoformat() if post.created_at else None,
            }
            for post in db_posts
        ]

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

    finally:
        session.close()


if __name__ == "__main__":
    app.run(debug=True)
