import json
from bluesky import BlueSkyManager
from utils.text_cleaner import TextCleaner
from sentiment_analyzer import SentimentAnalyzer


def main():
    # Retrieve posts using BlueSkyManager
    bs_manager = BlueSkyManager()
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

        # Optionally, attach sentiment results back to each post
        for post, sentiment in zip(posts, sentiment_results):
            post["sentiment"] = sentiment["label"]
            post["confidence"] = sentiment["score"]

        # Print out the sentiment results for verification
        for idx, (text, sentiment) in enumerate(
            zip(cleaned_texts, sentiment_results), 1
        ):
            print(f"\nPost {idx}:")
            print("Cleaned Text:", text)
            print("Sentiment:", sentiment)
    else:
        print("No posts found in the JSON file.")


if __name__ == "__main__":
    main()
