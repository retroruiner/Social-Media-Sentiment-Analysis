import re
import contractions
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


class TextCleaner:
    """
    A class to clean text data for further processing.
    """

    def __init__(self):
        logging.info("TextCleaner initialized.")

    def clean_text(self, text: str) -> str:
        logging.debug(f"Original text: {text}")

        # Expand contractions safely
        try:
            text = contractions.fix(text)
        except IndexError as e:
            logging.error(
                f"Failed expanding contractions for text: '{text}'. Error: {e}"
            )

            pass

        # Remove URLs
        text = re.sub(r"http\S+|www\.\S+|\.com|\.org|\.net|\.bsky\.social", "", text)
        logging.debug(f"After removing URLs: {text}")

        # Remove mentions and hashtags
        text = re.sub(r"[@#]\w+", "", text)
        logging.debug(f"After removing mentions and hashtags: {text}")

        # Keep only letters, numbers, and limited punctuation
        allowed_chars = r"a-zA-Z0-9\s" + re.escape("!?.,'")
        text = re.sub(rf"[^{allowed_chars}]", "", text)
        logging.debug(f"After filtering allowed characters: {text}")

        # Convert to lowercase
        text = text.lower()
        logging.debug(f"After converting to lowercase: {text}")

        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text).strip()
        logging.debug(f"After removing extra whitespace: {text}")

        return text
