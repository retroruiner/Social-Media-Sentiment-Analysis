import logging
import datetime
from datetime import timezone

from sqlalchemy.exc import IntegrityError
from src.db import Session
from src.models import Post
from src.bluesky_manager import BlueSkyManager
from src.utils.text_cleaner import TextCleaner
from src.sentiment_analyzer import SentimentAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

DEFAULT_QUERY = "Macron"


def determine_query_and_cleanup(session):
    """
    1) Fetch the two most recent rows with uri=="query".
    2) If both exist and their .query differs -> truncate the table and return newest.
    3) Otherwise (one or two matching) -> delete only the query rows and return the newest (or only).
    4) If none exist -> return DEFAULT_QUERY.
    """
    # 1) get up to 2 latest "query" posts
    query_rows = (
        session.query(Post)
        .filter(Post.uri == "query")
        .order_by(Post.created_at.desc())
        .limit(2)
        .all()
    )

    if len(query_rows) >= 2:
        latest, previous = query_rows[0], query_rows[1]
        if latest.query != previous.query:
            logging.info(
                f"Detected query change ('{previous.query}' → '{latest.query}'). "
                "Truncating entire posts table."
            )
            session.query(Post).delete()
            session.commit()
            return latest.query

        # they match
        logging.info(f"Query '{latest.query}' unchanged. Removing old query markers.")
        session.query(Post).filter(Post.uri == "query").delete()
        session.commit()
        return latest.query

    elif len(query_rows) == 1:
        only = query_rows[0]
        logging.info(f"Single query marker found: '{only.query}'. Removing it.")
        session.query(Post).filter(Post.uri == "query").delete()
        session.commit()
        return only.query

    else:
        logging.info(f"No query markers found; defaulting to '{DEFAULT_QUERY}'.")
        return DEFAULT_QUERY


def main():
    session = Session()

    # Decide which query to use and clean up markers/tables accordingly
    current_query = determine_query_and_cleanup(session)
    logging.info(f"▶ Using query: {current_query!r}")

    # Now gather existing URIs (after any truncation above)
    existing_uris = {uri for (uri,) in session.query(Post.uri).all()}
    logging.info(f"Found {len(existing_uris)} existing URIs in DB.")

    # Fetch from BlueSky
    bs = BlueSkyManager()
    bs.login()
    data = bs.get_posts(current_query)
    posts = data.get("posts", [])
    logging.info(f"Fetched {len(posts)} posts for query={current_query!r}")

    if not posts:
        logging.info("No posts to process; exiting.")
        session.close()
        return

    cleaner = TextCleaner()
    analyzer = SentimentAnalyzer()
    new_count = 0

    for p in posts:
        uri = p.get("uri")
        if not uri or uri in existing_uris:
            continue

        clean_text = cleaner.clean_text(p.get("text", ""))
        result = analyzer.analyze_texts([clean_text])
        if not result:
            logging.warning(f"Skipping {uri!r}; sentiment analysis returned nothing.")
            continue

        label = result[0].get("label")
        score = result[0].get("score")
        ts_str = p.get("createdAt")
        created_at = None
        if ts_str:
            try:
                created_at = datetime.datetime.fromisoformat(
                    ts_str.replace("Z", "+00:00")
                )
            except ValueError:
                logging.warning(f"Bad timestamp {ts_str!r} for {uri!r}")

        # Insert, tagging it with current_query
        db_post = Post(
            uri=uri,
            text=clean_text,
            sentiment=label,
            confidence=score,
            created_at=created_at,
            query=current_query,
        )
        session.add(db_post)
        try:
            session.commit()
            new_count += 1
        except IntegrityError:
            session.rollback()

    session.close()
    logging.info(f"Done. Inserted {new_count} new posts (query={current_query!r}).")


if __name__ == "__main__":
    main()
