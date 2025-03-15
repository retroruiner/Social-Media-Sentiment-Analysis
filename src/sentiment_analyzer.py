from transformers import pipeline


class SentimentAnalyzer:
    """
    A class for performing sentiment analysis using Hugging Face Transformers.
    """

    def __init__(
        self, model_name: str = "distilbert-base-uncased-finetuned-sst-2-english"
    ):
        """
        Initializes the sentiment analysis pipeline.

        Args:
            model_name (str): The Hugging Face model name to use for sentiment analysis.
                              Defaults to 'distilbert-base-uncased-finetuned-sst-2-english'.
        """
        self.pipeline = pipeline("sentiment-analysis", model=model_name)

    def analyze_texts(self, texts: list[str]) -> list:
        """
        Analyzes sentiment for a list of texts.

        Args:
            texts (list): A list of text strings to analyze.

        Returns:
            list: A list of dictionaries with sentiment analysis results for each text.
        """
        results = self.pipeline(texts)
        return results
