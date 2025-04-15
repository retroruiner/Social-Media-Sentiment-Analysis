# models.py
from sqlalchemy import Column, Integer, String, Date, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class DailySentimentSummary(Base):
    __tablename__ = "daily_sentiment_summary"

    id = Column(Integer, primary_key=True)
    query = Column(String)
    date = Column(Date)
    sentiment_distribution = Column(JSON)
    aggregate_by_date = Column(JSON)
    sentiment_over_time = Column(JSON)
    word_frequency = Column(JSON)
    heatmap_data = Column(JSON)
    top_words_by_sentiment = Column(JSON)
