from flask import Blueprint, jsonify, request
import json
import logging
import pandas as pd
from datetime import datetime
from bluesky_manager import BlueSkyManager
from utils.text_cleaner import TextCleaner
from sentiment_analyzer import SentimentAnalyzer
from data_processor import DataProcessor

# SQLAlchemy imports from your models
from database.models import DailySentimentSummary, Session, Base, engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

api = Blueprint("api", __name__)


@api.route("/daily_summary", methods=["GET"])
def daily_summary():
    """
    Retrieve all stored daily sentiment summaries from PostgreSQL.
    This endpoint lets your dashboard aggregate and display historical data.
    """
    session = Session()
    try:
        summaries = session.query(DailySentimentSummary).all()
        summaries_data = [
            {
                "id": summary.id,
                "query": summary.query,
                "date": summary.date.isoformat(),
                "sentiment_distribution": summary.sentiment_distribution,
                "aggregate_by_date": summary.aggregate_by_date,
                "sentiment_over_time": summary.sentiment_over_time,
                "word_frequency": summary.word_frequency,
                "heatmap_data": summary.heatmap_data,
                "top_words_by_sentiment": summary.top_words_by_sentiment,
            }
            for summary in summaries
        ]
        return jsonify(summaries_data)
    except Exception as e:
        logging.exception("Error retrieving daily summaries:")
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()
