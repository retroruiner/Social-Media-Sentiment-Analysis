#!/usr/bin/env python3

import argparse
import logging
import datetime
from datetime import timezone

from sqlalchemy.exc import IntegrityError
from src.db import Session
from src.models import Post
from src.bluesky_manager import BlueSkyManager
from src.utils.text_cleaner import TextCleaner
from src.sentiment_analyzer import SentimentAnalyzer

# -----------------------------------------------------------------------------
# Configuration & Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


# -----------------------------------------------------------------------------
# Main fetch & store logic
# -----------------------------------------------------------------------------
def main(query: str):
    logging.info(f"Starting fetch_and_store for query={query!r}")

    # 1) Fetch existing URIs so we can skip duplicates
    session = Session()
    existing_uris = {uri for (uri,) in session.query(Post.uri).all()}
    logging.info(f"Found {len(existing_uris)} existing posts in DB")

    # 2) Fetch from BlueSky
    bs = BlueSkyManager()
    bs.login()
    data = bs.get_posts(query)
    posts = data.get("posts", [])
    logging.info(f"Fetched {len(posts)} posts from BlueSky")

    if not posts:
        logging.info("No posts to process. Exiting.")
        session.close()
        return

    # 3) Prepare cleaning & analysis
    cleaner = TextCleaner()
    analyzer = SentimentAnalyzer()

    new_count = 0

    # 4) Process each post
    for post in posts:
        uri = post.get("uri")
        if not uri or uri in existing_uris:
            continue  # skip duplicates or missing URI

        raw_text = post.get("text", "")
        clean_text = cleaner.clean_text(raw_text)

        # Sentiment analysis (runs locally on GH Actions runner)
        result = analyzer.analyze_texts([clean_text])
        if not result:
            logging.warning(f"No sentiment result for uri={uri!r}; skipping.")
            continue
        sentiment = result[0].get("label")
        confidence = result[0].get("score")

        # Parse createdAt timestamp
        created_at = None
        created_str = post.get("createdAt")
        if created_str:
            try:
                created_at = datetime.datetime.fromisoformat(
                    created_str.replace("Z", "+00:00")
                )
            except ValueError:
                logging.warning(f"Invalid createdAt format {created_str!r}")

        # Build and insert Post object
        db_post = Post(
            uri=uri,
            text=clean_text,
            sentiment=sentiment,
            confidence=confidence,
            created_at=created_at,
        )
        session.add(db_post)
        try:
            session.commit()
            new_count += 1
            logging.debug(f"Inserted new post uri={uri!r}")
        except IntegrityError:
            session.rollback()
            logging.debug(f"Duplicate uri={uri!r} detected on commit; rolled back.")

    session.close()
    logging.info(f"Done. Inserted {new_count} new posts.")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch BlueSky posts, run sentiment analysis, store into DB"
    )
    parser.add_argument(
        "--query",
        type=str,
        default="Macron",
        help="Search query for BlueSky (default: %(default)s)",
    )
    args = parser.parse_args()
    main(args.query)
