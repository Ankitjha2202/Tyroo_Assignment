import os
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import uuid
from config import Config
from utils.logger import setup_logger, log_execution_time

logger = setup_logger(__name__)

class DataProcessor:
    def __init__(self):
        self.processed_rows = 0
        self.total_rows = 0
        self.batch_id = str(uuid.uuid4())
    
    @log_execution_time
    def download_file(self, url: str, file_path: str) -> bool:
        try:
            import requests
            
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            logger.info(f"Downloading file from: {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            
            logger.info(f"File downloaded successfully to: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download file: {str(e)}")
            return False
    
    def read_csv_in_chunks(self, file_path: str, chunk_size: int = None) -> pd.DataFrame:
        chunk_size = chunk_size or Config.CHUNK_SIZE
        chunks = []
        
        try:
            logger.info(f"Reading CSV file in chunks: {file_path}")
            
            compression = None
            if file_path.endswith('.gz'):
                compression = 'gzip'
            
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, compression=compression):
                chunks.append(chunk)
                logger.debug(f"Read chunk with {len(chunk)} rows")
            
            if chunks:
                combined_df = pd.concat(chunks, ignore_index=True)
                self.total_rows = len(combined_df)
                logger.info(f"Total rows read: {self.total_rows}")
                return combined_df
            else:
                logger.warning("No data found in CSV file")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to read CSV file: {str(e)}")
            raise
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        logger.info("Starting data cleaning process")
        
        original_count = len(df)
        
        df = df.copy()
        
        df = self._remove_duplicates(df)
        df = self._handle_missing_values(df)
        df = self._convert_data_types(df)
        
        df['source_file'] = os.path.basename(Config.DOWNLOAD_PATH)
        df['processing_batch'] = self.batch_id
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        self.processed_rows = len(df)
        
        final_count = len(df)
        logger.info(f"Data cleaning completed. Rows: {original_count} -> {final_count}")
        
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        original_count = len(df)
        df_cleaned = df.drop_duplicates()
        removed_count = original_count - len(df_cleaned)
        logger.info(f"Removed {removed_count} duplicate rows")
        return df_cleaned
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        null_counts = df.isnull().sum()
        logger.info(f"Null value counts:\n{null_counts}")
        
        for column in df.columns:
            if df[column].dtype in ['int64', 'float64']:
                median_val = df[column].median()
                df.loc[:, column] = df[column].fillna(median_val)
            else:
                mode_value = df[column].mode()
                if not mode_value.empty:
                    df.loc[:, column] = df[column].fillna(mode_value.iloc[0])
                else:
                    df.loc[:, column] = df[column].fillna('Unknown')
        
        return df
    
    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        for column in df.columns:
            if column in ['source_file', 'processing_batch', 'created_at', 'updated_at']:
                continue
            
            if df[column].dtype == 'object':
                numeric_converted = pd.to_numeric(df[column], errors='coerce')
                if not numeric_converted.isna().all():
                    df.loc[:, column] = numeric_converted
            
            if df[column].dtype in ['int64']:
                df.loc[:, column] = pd.to_numeric(df[column], downcast='integer')
            
            if df[column].dtype in ['float64']:
                df.loc[:, column] = pd.to_numeric(df[column], downcast='float')
        
        return df
    
    def validate_data(self, df: pd.DataFrame) -> Dict[str, int]:
        metrics = {
            'total_records': len(df),
            'valid_records': len(df.dropna()),
            'null_records': df.isnull().sum().sum(),
            'duplicate_records': len(df) - len(df.drop_duplicates())
        }
        
        logger.info(f"Data quality metrics: {metrics}")
        return metrics
    
    def get_processing_summary(self) -> Dict[str, any]:
        return {
            'batch_id': self.batch_id,
            'total_rows': self.total_rows,
            'processed_rows': self.processed_rows,
            'processing_time': datetime.now(),
            'success_rate': (self.processed_rows / self.total_rows * 100) if self.total_rows > 0 else 0
        } 