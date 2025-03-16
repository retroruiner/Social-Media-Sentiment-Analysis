import json
from bluesky_manager import BlueSkyManager
from utils.text_cleaner import TextCleaner
from sentiment_analyzer import SentimentAnalyzer
from data_processor import DataProcessor


def main():
    # Retrieve posts using BlueSkyManagerd
    bs_manager = BlueSkyManager()

    bs_manager.login()

    file_path = bs_manager.get_posts("Macron")
    print(f"Posts saved to: {file_path}")

    # Load the JSON file containing posts
    with open(file_path, "r") as infile:
        data = json.load(infile)

    posts = data.get("posts", [])
    if posts:
        cleaner = TextCleaner()
        analyzer = SentimentAnalyzer()

        # Collect cleaned texts from all posts
        cleaned_texts = []
        for post in posts:
            raw_text = post.get("text", "")
            cleaned = cleaner.clean_text(raw_text)
            cleaned_texts.append(cleaned)

        # Analyze sentiment for all cleaned texts in a batch
        sentiment_results = analyzer.analyze_texts(cleaned_texts)

        # Attach sentiment results back to each post
        for post, sentiment, clean in zip(posts, sentiment_results, cleaned_texts):
            post["sentiment"] = sentiment["label"]
            post["confidence"] = sentiment["score"]
            post["cleaned_text"] = clean

        # Print out the sentiment results for verification
        for idx, (text, sentiment) in enumerate(
            zip(cleaned_texts, sentiment_results), 1
        ):
            print(f"\nPost {idx}:")
            print("Cleaned Text:", text)
            print("Sentiment:", sentiment)

        # --------------------------------------------------
        # Data Processing using DataProcessor
        # --------------------------------------------------
        processor = DataProcessor(posts)

        # Get sentiment distribution
        sentiment_distribution = processor.get_sentiment_distribution()
        print("\nSentiment Distribution:")
        print(sentiment_distribution)

        # Aggregate posts by date (returns a DataFrame)
        date_aggregation = processor.aggregate_by_date()
        print("\nPosts Aggregated by Date:")
        print(date_aggregation)

        # Aggregate sentiment by date for time series analysis
        sentiment_time_series = processor.aggregate_sentiment_by_date()
        print("\nSentiment Over Time Data:")
        print(sentiment_time_series)

        # Get word frequency analysis
        word_frequency = processor.get_word_frequency()
        print("\nWord Frequency:")
        print(word_frequency)

        length_sentiment_df = processor.analyze_text_length_sentiment()
        print("\nText Length vs. Sentiment Data:")
        print(length_sentiment_df)
    else:
        print("No posts found in the JSON file.")


if __name__ == "__main__":
    main()
