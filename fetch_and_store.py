import os
import logging
import datetime
from datetime import timezone

from sqlalchemy.exc import IntegrityError
from src.db import Session
from src.models import (
    Post,
)
from src.bluesky_manager import BlueSkyManager
from src.utils.text_cleaner import TextCleaner
from src.sentiment_analyzer import SentimentAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

DEFAULT_QUERY = "Macron"


def get_latest_db_query(session):
    """Return the most recent Post.query for rows with uri=='query'."""
    row = (
        session.query(Post.query, Post.created_at)
        .filter(Post.uri == "query")
        .order_by(Post.created_at.desc())
        .first()
    )
    return row.query if row else None


def resolve_current_query(session):
    """
    Determine which query to use, in this order:
      1) If CURRENT_QUERY env-var is unset, try the DB’s latest 'uri=="query"' row.
      2) If still unset, fall back to DEFAULT_QUERY.
      3) If env-var and DB disagree, purge table and adopt the DB value.
    """
    env_q = os.getenv("CURRENT_QUERY")
    db_q = get_latest_db_query(session)

    # 1) If no env var, but DB has a query → use it
    if not env_q and db_q:
        logging.info(f"No CURRENT_QUERY env-var; using DB value '{db_q}'.")
        return db_q

    # 2) If neither env nor DB → default
    if not env_q and not db_q:
        logging.info(
            f"No CURRENT_QUERY or DB query found; defaulting to '{DEFAULT_QUERY}'."
        )
        return DEFAULT_QUERY

    # 3) Env exists, DB missing → keep env
    if env_q and not db_q:
        logging.info(f"CURRENT_QUERY env-var '{env_q}' set; no DB override found.")
        return env_q

    # 4) Both exist but differ → DB wins, and clear out everything
    if env_q != db_q:
        logging.info(
            f"Env-var '{env_q}' ≠ DB '{db_q}'; truncating table and using DB value."
        )
        session.query(Post).delete()
        session.commit()
        return db_q

    # 5) Both exist and match → just use it
    logging.info(f"Using CURRENT_QUERY='{env_q}' (matches DB).")
    return env_q


def export_to_github_env(var_name: str, value: str):
    """
    If running under GitHub Actions, append VAR=VALUE to the $GITHUB_ENV
    so downstream steps see it.
    """
    gh_env = os.getenv("GITHUB_ENV")
    if gh_env:
        with open(gh_env, "a") as fh:
            fh.write(f"{var_name}={value}\n")
        logging.info(f"Exported {var_name}='{value}' to $GITHUB_ENV")


def main():
    session = Session()

    # Figure out which query we should be using this run
    current_query = resolve_current_query(session)

    # Make sure subsequent steps in GH Actions see the final query
    export_to_github_env("CURRENT_QUERY", current_query)

    # Fetch existing URIs (will be empty if we just truncated)
    existing_uris = {uri for (uri,) in session.query(Post.uri).all()}
    logging.info(f"{len(existing_uris)} existing URIs in DB")

    # Fetch from BlueSky
    bs = BlueSkyManager()
    bs.login()
    data = bs.get_posts(current_query)
    posts = data.get("posts", [])
    logging.info(f"Fetched {len(posts)} posts for query={current_query!r}")

    if not posts:
        logging.info("No posts returned; exiting.")
        session.close()
        return

    cleaner = TextCleaner()
    analyzer = SentimentAnalyzer()
    new_count = 0

    for p in posts:
        uri = p.get("uri")
        if not uri or uri in existing_uris:
            continue

        text = cleaner.clean_text(p.get("text", ""))
        result = analyzer.analyze_texts([text])
        if not result:
            logging.warning(f"Skipping {uri!r}; no sentiment result.")
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

        db_post = Post(
            uri=uri,
            text=text,
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
