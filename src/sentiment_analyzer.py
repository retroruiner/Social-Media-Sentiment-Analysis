import logging
import torch
from transformers import pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


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
        logging.info(f"Initializing SentimentAnalyzer with model: {model_name}")

        # Check if GPU is available and set device accordingly
        device = 0 if torch.cuda.is_available() else -1
        logging.info(f"CUDA: {torch.cuda.is_available()}")
        logging.info(f"PyTorch version: {torch.__version__}")
        if torch.cuda.is_available():
            logging.info(f"GPU count: {torch.cuda.device_count()}")
            logging.info(f"GPU name: {torch.cuda.get_device_name(0)}")
        logging.info(f"Using device: {'GPU' if device == 0 else 'CPU'}")

        self.pipeline = pipeline("sentiment-analysis", model=model_name, device=device)
        logging.info("Sentiment analysis pipeline initialized successfully.")

    def analyze_texts(self, texts: list[str]) -> list:
        """
        Analyzes sentiment for a list of texts.

        Args:
            texts (list): A list of text strings to analyze.

        Returns:
            list: A list of dictionaries with sentiment analysis results for each text.
        """
        logging.info(f"Analyzing sentiment for {len(texts)} texts.")
        try:
            # For better GPU performance with multiple texts, specify batch_size
            results = self.pipeline(texts, batch_size=8)
            logging.info("Sentiment analysis completed successfully.")
        except Exception as e:
            logging.error(f"Error during sentiment analysis: {e}")
            results = []
        return results
