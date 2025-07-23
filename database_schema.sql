-- Database Schema for Data Processing Application
-- This schema is designed to store processed CSV data efficiently
-- SQLite compatible schema (default)

-- Create the main table for processed data
CREATE TABLE IF NOT EXISTS processed_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- CSV data columns
    platform_commission_rate REAL,
    venture_category3_name_en TEXT,
    product_small_img TEXT,
    deeplink TEXT,
    availability TEXT,
    image_url_5 REAL,
    number_of_reviews INTEGER,
    is_free_shipping INTEGER,
    promotion_price REAL,
    venture_category2_name_en TEXT,
    current_price REAL,
    product_medium_img TEXT,
    venture_category1_name_en TEXT,
    brand_name TEXT,
    image_url_4 REAL,
    description REAL,
    seller_url TEXT,
    product_commission_rate REAL,
    product_name TEXT,
    sku_id INTEGER,
    seller_rating REAL,
    bonus_commission_rate REAL,
    business_type TEXT,
    business_area TEXT,
    image_url_2 TEXT,
    discount_percentage REAL,
    seller_name TEXT,
    product_url TEXT,
    product_id INTEGER,
    venture_category_name_local TEXT,
    rating_avg_value REAL,
    product_big_img TEXT,
    image_url_3 REAL,
    price REAL,
    
    -- Metadata columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file TEXT,
    processing_batch TEXT
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_processed_data_created_at ON processed_data(created_at);
CREATE INDEX IF NOT EXISTS idx_processed_data_source_file ON processed_data(source_file);
CREATE INDEX IF NOT EXISTS idx_processed_data_product_id ON processed_data(product_id);
CREATE INDEX IF NOT EXISTS idx_processed_data_sku_id ON processed_data(sku_id);
CREATE INDEX IF NOT EXISTS idx_processed_data_brand_name ON processed_data(brand_name);
CREATE INDEX IF NOT EXISTS idx_processed_data_seller_name ON processed_data(seller_name);

-- Create a table to track processing status
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
);

-- Create indexes for processing log
CREATE INDEX IF NOT EXISTS idx_processing_log_status ON processing_log(status);
CREATE INDEX IF NOT EXISTS idx_processing_log_started_at ON processing_log(started_at);

-- Create a table for data quality metrics
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    total_records INTEGER,
    valid_records INTEGER,
    null_records INTEGER,
    duplicate_records INTEGER,
    processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for data quality metrics
CREATE INDEX IF NOT EXISTS idx_data_quality_batch_id ON data_quality_metrics(batch_id);
CREATE INDEX IF NOT EXISTS idx_data_quality_date ON data_quality_metrics(processing_date);

-- SQLite doesn't support the same trigger syntax as PostgreSQL
-- The updated_at column will be managed by the application code 