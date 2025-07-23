import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Any
from config import Config
from utils.logger import setup_logger

logger = setup_logger(__name__)

class DatabaseManager:
    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config.DATABASE_URL
        self.engine = None
        self._connect()
    
    def _connect(self):
        try:
            self.engine = create_engine(self.database_url)
            logger.info(f"Connected to database: {self.database_url}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def create_tables(self, schema_file: str = "database_schema.sql"):
        try:
            with self.engine.connect() as connection:
                processed_data_sql = """
                CREATE TABLE IF NOT EXISTS processed_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform_commission_rate TEXT,
                    venture_category3_name_en TEXT,
                    product_small_img TEXT,
                    deeplink TEXT,
                    availability TEXT,
                    image_url_5 TEXT,
                    number_of_reviews TEXT,
                    is_free_shipping TEXT,
                    promotion_price TEXT,
                    venture_category2_name_en TEXT,
                    current_price TEXT,
                    product_medium_img TEXT,
                    venture_category1_name_en TEXT,
                    brand_name TEXT,
                    image_url_4 TEXT,
                    description TEXT,
                    seller_url TEXT,
                    product_commission_rate TEXT,
                    product_name TEXT,
                    sku_id TEXT,
                    seller_rating TEXT,
                    bonus_commission_rate TEXT,
                    business_type TEXT,
                    business_area TEXT,
                    image_url_2 TEXT,
                    discount_percentage TEXT,
                    seller_name TEXT,
                    product_url TEXT,
                    product_id TEXT,
                    venture_category_name_local TEXT,
                    rating_avg_value TEXT,
                    product_big_img TEXT,
                    image_url_3 TEXT,
                    price TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_file TEXT,
                    processing_batch TEXT
                )
                """
                connection.execute(text(processed_data_sql))
                
                processing_log_sql = """
                CREATE TABLE IF NOT EXISTS processing_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT NOT NULL,
                    total_rows INTEGER,
                    processed_rows INTEGER,
                    status TEXT DEFAULT 'pending',
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    processing_time_seconds REAL
                )
                """
                connection.execute(text(processing_log_sql))
                
                quality_metrics_sql = """
                CREATE TABLE IF NOT EXISTS data_quality_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    total_records INTEGER,
                    valid_records INTEGER,
                    null_records INTEGER,
                    duplicate_records INTEGER,
                    processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                connection.execute(text(quality_metrics_sql))
                
                indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_processed_data_created_at ON processed_data(created_at)",
                    "CREATE INDEX IF NOT EXISTS idx_processed_data_source_file ON processed_data(source_file)",
                    "CREATE INDEX IF NOT EXISTS idx_processing_log_status ON processing_log(status)",
                    "CREATE INDEX IF NOT EXISTS idx_processing_log_started_at ON processing_log(started_at)",
                    "CREATE INDEX IF NOT EXISTS idx_data_quality_batch_id ON data_quality_metrics(batch_id)",
                    "CREATE INDEX IF NOT EXISTS idx_data_quality_date ON data_quality_metrics(processing_date)"
                ]
                
                for index_sql in indexes:
                    connection.execute(text(index_sql))
                
                connection.commit()
            
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
        except Exception as e:
            logger.error(f"Failed to check table existence: {str(e)}")
            return False
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        batch_size: int = None) -> int:
        batch_size = batch_size or Config.BATCH_SIZE
        total_inserted = 0
        
        try:
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i + batch_size]
                
                batch_df.to_sql(
                    table_name,
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                total_inserted += len(batch_df)
                logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch_df)} rows")
            
            logger.info(f"Total rows inserted: {total_inserted}")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Failed to insert data: {str(e)}")
            raise
    
    def log_processing_status(self, file_name: str, total_rows: int, 
                            processed_rows: int, status: str, 
                            error_message: str = None, 
                            processing_time: float = None):
        try:
            query = """
            INSERT INTO processing_log 
            (file_name, total_rows, processed_rows, status, error_message, processing_time_seconds)
            VALUES (:file_name, :total_rows, :processed_rows, :status, :error_message, :processing_time)
            """
            
            with self.engine.connect() as connection:
                connection.execute(text(query), {
                    'file_name': file_name,
                    'total_rows': total_rows,
                    'processed_rows': processed_rows,
                    'status': status,
                    'error_message': error_message,
                    'processing_time': processing_time
                })
                connection.commit()
            
            logger.info(f"Processing status logged: {file_name} - {status}")
            
        except Exception as e:
            logger.error(f"Failed to log processing status: {str(e)}")
    
    def log_data_quality_metrics(self, batch_id: str, total_records: int,
                               valid_records: int, null_records: int,
                               duplicate_records: int):
        try:
            query = """
            INSERT INTO data_quality_metrics 
            (batch_id, total_records, valid_records, null_records, duplicate_records)
            VALUES (:batch_id, :total_records, :valid_records, :null_records, :duplicate_records)
            """
            
            with self.engine.connect() as connection:
                connection.execute(text(query), {
                    'batch_id': batch_id,
                    'total_records': total_records,
                    'valid_records': valid_records,
                    'null_records': null_records,
                    'duplicate_records': duplicate_records
                })
                connection.commit()
            
            logger.info(f"Data quality metrics logged for batch: {batch_id}")
            
        except Exception as e:
            logger.error(f"Failed to log data quality metrics: {str(e)}")
    
    def close(self):
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed") 