import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data_processing.db')
    
    CSV_URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
    DOWNLOAD_PATH = "data/downloaded_data.csv.gz"
    PROCESSED_DATA_PATH = "data/processed_data.csv"
    
    CHUNK_SIZE = 10000
    BATCH_SIZE = 1000
    
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = "logs/data_processing.log" 