import logging
import datetime

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
    1) Fetch the two most recent rows (by id DESC).
    2) If both exist and their .query differs --> truncate the entire table and return latest.query.
    3) Otherwise (one or both share the same .query) --> delete only the uri=='query' markers, return latest.query.
    4) If none exist --> return DEFAULT_QUERY.
    """
    latest_rows = session.query(Post).order_by(Post.id.desc()).limit(2).all()

    if len(latest_rows) >= 2:
        latest, previous = latest_rows[0], latest_rows[1]
        if latest.query != previous.query:
            logging.info(
                f"Detected query change (previous='{previous.query}' → latest='{latest.query}'). "
                "Truncating entire posts table."
            )
            session.query(Post).delete()
            session.commit()
            return latest.query

        logging.info(
            f"Query '{latest.query}' unchanged. Removing only query-marker rows."
        )
        session.query(Post).filter(Post.uri == "query").delete()
        session.commit()
        return latest.query

    elif len(latest_rows) == 1:
        only = latest_rows[0]
        logging.info(
            f"Only one row found (query='{only.query}'). Removing query-marker rows."
        )
        session.query(Post).filter(Post.uri == "query").delete()
        session.commit()
        return only.query

    else:
        logging.info(f"No rows in posts table; defaulting to '{DEFAULT_QUERY}'.")
        return DEFAULT_QUERY


def main():
    session = Session()

    # 1) Determine the query and clean up markers/tables
    current_query = determine_query_and_cleanup(session)
    logging.info(f"▶ Using query: {current_query!r}")

    # 2) Fetch existing URIs so we avoid duplicates
    existing_uris = {uri for (uri,) in session.query(Post.uri).all()}
    logging.info(f"Found {len(existing_uris)} existing URIs in DB.")

    # 3) Fetch new posts from BlueSky
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

    # 4) Collect all new posts into a batch
    to_process = []
    for p in posts:
        uri = p.get("uri")
        if not uri or uri in existing_uris:
            continue
        clean_text = cleaner.clean_text(p.get("text", ""))
        ts_str = p.get("createdAt")
        to_process.append((uri, clean_text, ts_str))

    if not to_process:
        logging.info("No new posts to analyze.")
        session.close()
        return

    uris, texts, timestamps = zip(*to_process)
    results = analyzer.analyze_texts(list(texts))

    # 5) Insert analyzed posts into the database
    new_count = 0
    for uri, clean_text, ts_str, result in zip(uris, texts, timestamps, results):
        if not result:
            logging.warning(f"Skipping {uri!r}; sentiment analysis returned nothing.")
            continue

        label = result.get("label")
        score = result.get("score")

        created_at = None
        if ts_str:
            try:
                created_at = datetime.datetime.fromisoformat(
                    ts_str.replace("Z", "+00:00")
                )
            except ValueError:
                logging.warning(f"Bad timestamp {ts_str!r} for {uri!r}")

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
