#!/usr/bin/env python3

import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from utils.logger import setup_logger
from utils.database import DatabaseManager
from utils.data_processor import DataProcessor

logger = setup_logger(__name__)

def show_sqlite_setup():
    logger.info("🔧 SQLite Setup (Super Simple!)")
    logger.info("")
    logger.info("✅ What you get:")
    logger.info("   - Database file: data_processing.db (created automatically)")
    logger.info("   - No installation needed (SQLite comes with Python)")
    logger.info("   - No configuration needed")
    logger.info("   - No server setup")
    logger.info("   - Just runs from your project folder")
    logger.info("")

def run_simple_pipeline():
    logger.info("🚀 Running Simple SQLite Pipeline")
    logger.info("")
    
    try:
        from data_processor import DataProcessingPipeline
        
        pipeline = DataProcessingPipeline()
        summary = pipeline.run_pipeline()
        
        logger.info("")
        logger.info("🎉 Pipeline completed successfully!")
        logger.info(f"📊 Processed {summary['inserted_rows']:,} rows")
        logger.info(f"⏱️  Total time: {summary['processing_time']:.2f} seconds")
        logger.info(f"📈 Success rate: {summary['success_rate']:.1f}%")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        return False

def show_database_info():
    logger.info("")
    logger.info("🗄️ Database Info:")
    
    try:
        import sqlite3
        
        if os.path.exists('data_processing.db'):
            conn = sqlite3.connect('data_processing.db')
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM processed_data")
            row_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            
            logger.info(f"   📁 Database file: data_processing.db")
            logger.info(f"   📊 Tables: {', '.join(tables)}")
            logger.info(f"   📈 Total rows: {row_count:,}")
            logger.info("   🔍 You can query with: sqlite3 data_processing.db")
            
        else:
            logger.info("   📁 Database file: data_processing.db (not created yet)")
            
    except Exception as e:
        logger.error(f"   ❌ Error checking database: {str(e)}")

def main():
    show_sqlite_setup()
    
    if run_simple_pipeline():
        show_database_info()
    
    logger.info("")
    logger.info("✨ That's it! Simple and clean.")

if __name__ == "__main__":
    main() 