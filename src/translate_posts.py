from googletrans import Translator


class PostTranslator:
    def __init__(self):
        self.translator = Translator()

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
        try:
            detection = await self.translator.detect(text)
        except Exception as e:
            print(f"Language detection error: {e}")
            return {"text": text, "language": "unknown"}

        if detection.lang != "en":
            try:
                translation = await self.translator.translate(text, dest="en")
                return {"text": translation.text, "language": "machine-en"}
            except Exception as e:
                print(f"Translation error: {e}")
                return {"text": text, "language": detection.lang}
        else:
            return {"text": text, "language": "en"}
