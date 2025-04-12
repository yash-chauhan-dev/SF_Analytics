import logging
import os
from tqdm import tqdm
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

print("Pandas version:", pd.__version__)
# Load env vars
load_dotenv()

# Setup logging (no emojis for Windows compatibility)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/load_to_snowflake.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Connect to Snowflake
logging.info("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv('user'),
    password=os.getenv('password'),
    account=os.getenv('account'),
    warehouse=os.getenv('warehouse'),
    database=os.getenv('database'),
    schema=os.getenv('schema'),
    role=os.getenv('role')
)
logging.info("Snowflake connection established.")

# Load data
file_path = 'sf_config_data_load/data/2019-Nov.csv'
logging.info(f"Loading data from: {file_path}")
df = pd.read_csv(file_path, nrows=5000)

# Data prep
df.columns = [col.strip().upper() for col in df.columns]

# Convert 'event_time' to string (Snowflake-compatible format)
df['EVENT_TIME'] = pd.to_datetime(
    df['EVENT_TIME']).dt.strftime('%Y-%m-%d %H:%M:%S')

# Ensure correct column order (for sanity)
df = df[[
    'USER_ID', 'EVENT_TIME', 'EVENT_TYPE', 'PRODUCT_ID',
    'CATEGORY_ID', 'CATEGORY_CODE', 'BRAND', 'PRICE', 'USER_SESSION'
]]

logging.info(f"Dataframe prepared with {len(df)} records.")

# Chunked upload
chunk_size = 1000
chunks = [df[i:i+chunk_size] for i in range(0, df.shape[0], chunk_size)]
total_loaded = 0

logging.info("Starting upload to Snowflake...")
for i, chunk in enumerate(tqdm(chunks, desc="Uploading chunks", unit="chunk")):
    success, nchunks, nrows, _ = write_pandas(
        conn, chunk, table_name='USER_EVENTS')
    total_loaded += nrows
    logging.info(
        f"Chunk {i+1}/{len(chunks)}: Uploaded {nrows} rows. Success: {success}")

logging.info(f"Upload complete. Total rows loaded: {total_loaded}")
conn.close()
logging.info("Snowflake connection closed.")
