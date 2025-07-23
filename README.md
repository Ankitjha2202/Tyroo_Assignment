# Data Processing Pipeline

A Python application for processing large CSV files using Pandas and storing results in SQLite database.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python data_processor.py

# Or run the simple demo
python simple_demo.py
```

## What It Does

1. **Downloads** a large CSV file (1M+ rows) from Tyroo's server
2. **Processes** the data in chunks to handle memory efficiently
3. **Cleans** the data by removing duplicates and handling missing values
4. **Stores** everything in SQLite database
5. **Logs** all operations with detailed progress tracking

## Project Structure

```
Tyroo_Assignment/
├── data/                          # Data files
│   └── downloaded_data.csv.gz     # Downloaded CSV (550MB, 1M+ rows)
├── logs/                          # Log files
│   └── data_processing.log        # Processing logs
├── utils/                         # Core modules
│   ├── logger.py                  # Logging utilities
│   ├── database.py                # Database operations
│   └── data_processor.py         # Data processing utilities
├── config.py                      # Configuration settings
├── data_processor.py              # Main pipeline script
├── simple_demo.py                 # Simple demo script
├── database_schema.sql            # Database structure
├── requirements.txt               # Python dependencies
├── data_processing.db             # SQLite database (created automatically)
└── README.md                      # This file
```

## Usage

### Option 1: Run Full Pipeline
```bash
python data_processor.py
```

### Option 2: Run Simple Demo
```bash
python simple_demo.py
```

Both will:
- Download the CSV file (if not already downloaded)
- Process 1M+ rows of e-commerce data
- Store results in `data_processing.db`
- Show progress and final summary

## Data Storage

### Database File
- **Location**: `data_processing.db` (created automatically)
- **Size**: ~1.4GB after processing 1M+ rows
- **Type**: SQLite database

### Tables Created

#### 1. `processed_data` (Main table)
Contains all the processed CSV data with these columns:
- `product_name`, `current_price`, `seller_name`
- `brand_name`, `description`, `product_url`
- `venture_category1_name_en`, `venture_category2_name_en`
- `platform_commission_rate`, `product_commission_rate`
- `number_of_reviews`, `seller_rating`, `rating_avg_value`
- `is_free_shipping`, `promotion_price`, `discount_percentage`
- `business_type`, `business_area`, `seller_url`
- `product_id`, `sku_id`, `availability`
- `image_url_2`, `image_url_3`, `image_url_4`, `image_url_5`
- `product_small_img`, `product_medium_img`, `product_big_img`
- `deeplink`, `venture_category3_name_en`, `venture_category_name_local`
- `bonus_commission_rate`, `price`
- `source_file`, `processing_batch`, `created_at`, `updated_at`

#### 2. `processing_log` (Tracking table)
Tracks processing status:
- `file_name`, `total_rows`, `processed_rows`
- `status`, `started_at`, `completed_at`
- `error_message`, `processing_time_seconds`

#### 3. `data_quality_metrics` (Quality table)
Data quality statistics:
- `batch_id`, `total_records`, `valid_records`
- `null_records`, `duplicate_records`, `processing_date`

## Querying Your Data

### Using SQLite Command Line
```bash
# Connect to database
sqlite3 data_processing.db

# Check total rows
SELECT COUNT(*) FROM processed_data;

# View sample data
SELECT product_name, current_price, seller_name 
FROM processed_data LIMIT 5;

# Find expensive products
SELECT product_name, current_price, seller_name 
FROM processed_data 
WHERE current_price > 1000 
ORDER BY current_price DESC 
LIMIT 10;

# Check processing logs
SELECT * FROM processing_log ORDER BY started_at DESC;

# Exit
.quit
```

### Using Python
```python
import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('data_processing.db')

# Load data into pandas
df = pd.read_sql_query("SELECT * FROM processed_data", conn)

# Basic analysis
print(f"Total products: {len(df)}")
print(f"Average price: ${df['current_price'].mean():.2f}")
print(f"Unique sellers: {df['seller_name'].nunique()}")

# Filter data
expensive_products = df[df['current_price'] > 1000]
print(f"Expensive products: {len(expensive_products)}")

conn.close()
```

### Common Queries

#### Product Analysis
```sql
-- Top 10 most expensive products
SELECT product_name, current_price, seller_name 
FROM processed_data 
ORDER BY current_price DESC 
LIMIT 10;

-- Products with highest ratings
SELECT product_name, rating_avg_value, seller_name 
FROM processed_data 
WHERE rating_avg_value > 4.5 
ORDER BY rating_avg_value DESC 
LIMIT 10;

-- Products with free shipping
SELECT COUNT(*) as free_shipping_count 
FROM processed_data 
WHERE is_free_shipping = 1;
```

#### Seller Analysis
```sql
-- Top sellers by number of products
SELECT seller_name, COUNT(*) as product_count 
FROM processed_data 
GROUP BY seller_name 
ORDER BY product_count DESC 
LIMIT 10;

-- Average commission rates by seller
SELECT seller_name, 
       AVG(platform_commission_rate) as avg_commission 
FROM processed_data 
GROUP BY seller_name 
ORDER BY avg_commission DESC;
```

#### Category Analysis
```sql
-- Products by category
SELECT venture_category1_name_en, COUNT(*) as product_count 
FROM processed_data 
GROUP BY venture_category1_name_en 
ORDER BY product_count DESC;

-- Average prices by category
SELECT venture_category1_name_en, 
       AVG(current_price) as avg_price 
FROM processed_data 
GROUP BY venture_category1_name_en 
ORDER BY avg_price DESC;
```

## Configuration

Edit `config.py` to customize:

```python
class Config:
    # Processing settings
    CHUNK_SIZE = 10000    # Rows to process at once
    BATCH_SIZE = 1000     # Rows to insert per batch
    
    # Data source
    CSV_URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
    
    # Database
    DATABASE_URL = "sqlite:///data_processing.db"
    
    # Logging
    LOG_LEVEL = "INFO"
    LOG_FILE = "logs/data_processing.log"
```

## Performance

### Processing Time
- **1M+ rows**: ~4-5 minutes
- **Memory usage**: Optimized with chunking
- **Database size**: ~1.4GB

### Optimization Tips
- Increase `CHUNK_SIZE` for faster processing (if you have more RAM)
- Increase `BATCH_SIZE` for faster database inserts
- Use indexes for faster queries (already created automatically)

## Logs

### Log File Location
- **File**: `logs/data_processing.log`
- **Format**: Detailed timestamps and progress tracking

### What's Logged
- Download progress
- Data processing steps
- Database operations
- Error messages
- Performance metrics
- Data quality statistics

### Sample Log Output
```
2025-01-24 10:30:15 - INFO - Starting data processing pipeline
2025-01-24 10:30:16 - INFO - Downloading file from: https://...
2025-01-24 10:30:45 - INFO - File downloaded successfully
2025-01-24 10:30:46 - INFO - Reading CSV file in chunks
2025-01-24 10:31:20 - INFO - Total rows read: 1,001,001
2025-01-24 10:31:21 - INFO - Starting data cleaning process
2025-01-24 10:31:45 - INFO - Data cleaning completed
2025-01-24 10:32:10 - INFO - Inserted batch 1: 1,000 rows
...
2025-01-24 10:34:30 - INFO - Pipeline completed successfully
```

## Troubleshooting

### Common Issues

1. **Memory Error**
   - Reduce `CHUNK_SIZE` in `config.py`
   - Close other applications to free RAM

2. **Download Failed**
   - Check internet connection
   - Verify URL in `config.py`

3. **Database Error**
   - Delete `data_processing.db` and run again
   - Check disk space

4. **Permission Error**
   - Ensure write permissions for `data/` and `logs/` directories

### Debug Mode
Set `LOG_LEVEL = "DEBUG"` in `config.py` for detailed logging.

## Data Export

### Export to CSV
```python
import pandas as pd
import sqlite3

conn = sqlite3.connect('data_processing.db')
df = pd.read_sql_query("SELECT * FROM processed_data", conn)
df.to_csv('exported_data.csv', index=False)
conn.close()
```

### Export to Excel
```python
import pandas as pd
import sqlite3

conn = sqlite3.connect('data_processing.db')
df = pd.read_sql_query("SELECT * FROM processed_data", conn)
df.to_excel('exported_data.xlsx', index=False)
conn.close()
```

## Next Steps

After running the pipeline, you can:

1. **Analyze the data** using SQL queries
2. **Export to other formats** (CSV, Excel, JSON)
3. **Build visualizations** with the processed data
4. **Create reports** based on the e-commerce metrics
5. **Integrate with other tools** using the SQLite database

## Dependencies

- **pandas**: Data manipulation
- **sqlalchemy**: Database operations
- **requests**: File downloads
- **python-dotenv**: Environment variables
- **loguru**: Advanced logging

All dependencies are listed in `requirements.txt`.