from googletrans import Translator
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

for noisy_lib in ["httpx", "httpcore", "urllib3", "googletrans"]:
    logging.getLogger(noisy_lib).setLevel(logging.WARNING)


class PostTranslator:
    def __init__(self):
        self.translator = Translator()
        logging.info("PostTranslator initialized with Google Translator.")

    async def translate_text(self, text: str) -> dict:
        """
        Asynchronously detects the language of the text and translates it to English if necessary.

        Args:
            text (str): The cleaned text.

        Returns:
            dict: A dictionary containing:
                - "text": The (possibly translated) text.
                - "language": "machine-en" if translation occurred, else "en".
        """
        logging.debug("Starting translation process.")
        logging.debug(f"Original text: {text}")
        try:
            detection = await self.translator.detect(text)
            logging.debug(f"Language detected: {detection.lang}")
        except Exception as e:
            logging.error(f"Language detection error: {e}")
            return {"text": text, "language": "unknown"}

        if detection.lang != "en":
            try:
                translation = await self.translator.translate(text, dest="en")
                logging.debug("Translation completed successfully.")
                return {"text": translation.text, "language": "machine-en"}
            except Exception as e:
                logging.error(f"Translation error: {e}")
                return {"text": text, "language": detection.lang}
        else:
            logging.debug("No translation needed; text is already in English.")
            return {"text": text, "language": "en"}
