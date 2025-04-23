import nltk

# Download all required NLTK corpora unconditionally
for corpus in ("stopwords", "punkt", "wordnet"):
    print(f"Downloading NLTK corpus: {corpus}")
    nltk.download(corpus, quiet=True)

print("NLTK setup complete.")
