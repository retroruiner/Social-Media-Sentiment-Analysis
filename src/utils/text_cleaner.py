import re
import string


class TextCleaner:
    """
    A class to clean text data for further processing.
    """

    def __init__(self):
        pass

    def clean_text(self, text: str) -> str:
        """
        Cleans the provided text by:
        - Removing URLs
        - Removing mentions (e.g., @username) and hashtags (e.g., #topic)
        - Removing unwanted characters but preserving letters, whitespace, and punctuation
        - Converting the text to lowercase
        - Removing extra whitespace

        Args:
            text (str): The original text.

        Returns:
            str: The cleaned text.
        """
        # Remove URLs
        text = re.sub(r"http\S+|www\.\S+", "", text)
        # Remove mentions and hashtags
        text = re.sub(r"[@#]\w+", "", text)
        # Allow letters, whitespace, and punctuation:
        allowed_chars = r"a-zA-Z\s" + re.escape(string.punctuation)
        text = re.sub(rf"[^{allowed_chars}]", "", text)
        # Convert to lowercase
        text = text.lower()
        # Remove extra spaces
        text = re.sub(r"\s+", " ", text).strip()
        return text
