import re
import string
import contractions


class TextCleaner:
    """
    A class to clean text data for further processing.
    """

    def __init__(self):
        pass

    def clean_text(self, text: str) -> str:
        """
        Cleans text by:
        - Expanding contractions (e.g., "I'm" → "I am")
        - Removing URLs, mentions, and hashtags
        - Keeping only letters, numbers, and essential punctuation
        - Converting to lowercase
        - Removing excessive whitespace
        """
        # Expand contractions (e.g., "I'm" -> "I am")
        text = contractions.fix(text)

        # Remove URLs
        text = re.sub(r"http\S+|www\.\S+|\.com|\.org|\.net|\.bsky\.social", "", text)

        # Remove mentions and hashtags
        text = re.sub(r"[@#]\w+", "", text)

        # Keep only letters, numbers, and limited punctuation
        allowed_chars = r"a-zA-Z0-9\s" + re.escape("!?.,'")
        text = re.sub(rf"[^{allowed_chars}]", "", text)

        # Convert to lowercase
        text = text.lower()

        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text).strip()

        return text
