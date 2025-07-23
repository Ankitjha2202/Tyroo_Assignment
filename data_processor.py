#!/usr/bin/env python3

import os
import sys
import time
from datetime import datetime
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from utils.logger import setup_logger, log_execution_time
from utils.database import DatabaseManager
from utils.data_processor import DataProcessor

logger = setup_logger(__name__)

class DataProcessingPipeline:
    def __init__(self):
        self.data_processor = DataProcessor()
        self.db_manager = DatabaseManager()
        self.start_time = None
        self.end_time = None
    
    @log_execution_time
    def run_pipeline(self) -> Dict[str, Any]:
        try:
            self.start_time = datetime.now()
            logger.info("Starting data processing pipeline")
            
            if not self._download_file():
                raise Exception("Failed to download CSV file")
            
            df = self._read_and_process_data()
            if df.empty:
                raise Exception("No data found in CSV file")
            
            cleaned_df = self._clean_and_transform_data(df)
            quality_metrics = self._validate_data_quality(cleaned_df)
            inserted_rows = self._store_data_in_database(cleaned_df)
            self._log_processing_results(inserted_rows, quality_metrics)
            
            self.end_time = datetime.now()
            processing_time = (self.end_time - self.start_time).total_seconds()
            
            summary = self._generate_summary(inserted_rows, quality_metrics, processing_time)
            
            logger.info("Data processing pipeline completed successfully")
            return summary
            
        except Exception as e:
            self.end_time = datetime.now()
            processing_time = (self.end_time - self.start_time).total_seconds() if self.start_time else 0
            
            logger.error(f"Pipeline failed after {processing_time:.2f} seconds: {str(e)}")
            self._log_error(str(e), processing_time)
            raise
    
    def _download_file(self) -> bool:
        logger.info("Step 1: Downloading CSV file")
        
        if os.path.exists(Config.DOWNLOAD_PATH):
            logger.info(f"File already exists at {Config.DOWNLOAD_PATH}")
            return True
        
        success = self.data_processor.download_file(
            Config.CSV_URL, 
            Config.DOWNLOAD_PATH
        )
        
        if success:
            logger.info("CSV file downloaded successfully")
        else:
            logger.error("Failed to download CSV file")
        
        return success
    
    def _read_and_process_data(self):
        logger.info("Step 2: Reading CSV file in chunks")
        
        if not os.path.exists(Config.DOWNLOAD_PATH):
            raise FileNotFoundError(f"Downloaded file not found: {Config.DOWNLOAD_PATH}")
        
        df = self.data_processor.read_csv_in_chunks(Config.DOWNLOAD_PATH)
        
        if df.empty:
            raise ValueError("No data found in CSV file")
        
        logger.info(f"Successfully read {len(df)} rows from CSV file")
        return df
    
    def _clean_and_transform_data(self, df):
        logger.info("Step 3: Cleaning and transforming data")
        
        cleaned_df = self.data_processor.clean_data(df)
        
        logger.info(f"Data cleaning completed. Processed {len(cleaned_df)} rows")
        return cleaned_df
    
    def _validate_data_quality(self, df):
        logger.info("Step 4: Validating data quality")
        
        quality_metrics = self.data_processor.validate_data(df)
        
        # Log quality metrics to database
        self.db_manager.log_data_quality_metrics(
            batch_id=self.data_processor.batch_id,
            total_records=quality_metrics['total_records'],
            valid_records=quality_metrics['valid_records'],
            null_records=quality_metrics['null_records'],
            duplicate_records=quality_metrics['duplicate_records']
        )
        
        return quality_metrics
    
    def _store_data_in_database(self, df):
        logger.info("Step 5: Storing data in database")
        
        # Create tables if they don't exist
        if not self.db_manager.table_exists('processed_data'):
            logger.info("Creating database tables")
            self.db_manager.create_tables()
        
        # Insert data into database
        inserted_rows = self.db_manager.insert_dataframe(
            df, 
            'processed_data',
            Config.BATCH_SIZE
        )
        
        logger.info(f"Successfully inserted {inserted_rows} rows into database")
        return inserted_rows
    
    def _log_processing_results(self, inserted_rows, quality_metrics):
        logger.info("Step 6: Logging processing results")
        
        processing_time = (self.end_time - self.start_time).total_seconds()
        
        self.db_manager.log_processing_status(
            file_name=os.path.basename(Config.DOWNLOAD_PATH),
            total_rows=self.data_processor.total_rows,
            processed_rows=inserted_rows,
            status='completed',
            processing_time=processing_time
        )
    
    def _log_error(self, error_message, processing_time):
        """
        Log error to database.
        
        Args:
            error_message: Error message
            processing_time: Processing time in seconds
        """
        try:
            self.db_manager.log_processing_status(
                file_name=os.path.basename(Config.DOWNLOAD_PATH),
                total_rows=self.data_processor.total_rows,
                processed_rows=self.data_processor.processed_rows,
                status='failed',
                error_message=error_message,
                processing_time=processing_time
            )
        except Exception as e:
            logger.error(f"Failed to log error to database: {str(e)}")
    
    def _generate_summary(self, inserted_rows, quality_metrics, processing_time):
        """
        Generate processing summary.
        
        Args:
            inserted_rows: Number of rows inserted
            quality_metrics: Data quality metrics
            processing_time: Processing time in seconds
            
        Returns:
            Dict: Processing summary
        """
        summary = {
            'batch_id': self.data_processor.batch_id,
            'total_rows': self.data_processor.total_rows,
            'processed_rows': inserted_rows,
            'processing_time_seconds': processing_time,
            'success_rate': (inserted_rows / self.data_processor.total_rows * 100) if self.data_processor.total_rows > 0 else 0,
            'quality_metrics': quality_metrics,
            'status': 'completed',
            'start_time': self.start_time,
            'end_time': self.end_time
        }
        
        logger.info("Processing Summary:")
        logger.info(f"  Batch ID: {summary['batch_id']}")
        logger.info(f"  Total Rows: {summary['total_rows']}")
        logger.info(f"  Processed Rows: {summary['processed_rows']}")
        logger.info(f"  Processing Time: {summary['processing_time_seconds']:.2f} seconds")
        logger.info(f"  Success Rate: {summary['success_rate']:.2f}%")
        
        return summary
    
    def cleanup(self):
        """Clean up resources."""
        try:
            self.db_manager.close()
            logger.info("Cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup failed: {str(e)}")

def main():
    """Main function to run the data processing pipeline."""
    pipeline = DataProcessingPipeline()
    
    try:
        summary = pipeline.run_pipeline()
        print("\n" + "="*50)
        print("DATA PROCESSING COMPLETED SUCCESSFULLY")
        print("="*50)
        print(f"Batch ID: {summary['batch_id']}")
        print(f"Total Rows: {summary['total_rows']:,}")
        print(f"Processed Rows: {summary['processed_rows']:,}")
        print(f"Processing Time: {summary['processing_time_seconds']:.2f} seconds")
        print(f"Success Rate: {summary['success_rate']:.2f}%")
        print("="*50)
        
    except Exception as e:
        print("\n" + "="*50)
        print("DATA PROCESSING FAILED")
        print("="*50)
        print(f"Error: {str(e)}")
        print("Check logs for detailed information")
        print("="*50)
        sys.exit(1)
    
    finally:
        pipeline.cleanup()

if __name__ == "__main__":
    main() 