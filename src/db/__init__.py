import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../..", ".env"))

DATABASE_URI = os.getenv("DATABASE_URI")
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
