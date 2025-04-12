import snowflake.connector
import yaml
import os
import logging
from dotenv import load_dotenv

# Initialize environment variables
load_dotenv()

# --------------------- Logging Setup ---------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

# --------------------- Snowflake Metadata ---------------------


def get_snowflake_metadata():
    logging.info("Fetching Snowflake metadata...")

    SNOWFLAKE_CONFIG = {
        "account": os.getenv("account"),
        "user": os.getenv("user"),
        "password": os.getenv("password"),
        "warehouse": os.getenv("warehouse"),
        "database": os.getenv("database"),
        "schema": os.getenv("schema"),
        "role": os.getenv("role"),
    }

    try:
        ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cs = ctx.cursor()
        cs.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cs.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")

        cs.execute(f"""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{SNOWFLAKE_CONFIG['schema']}'
            ORDER BY table_schema, table_name, ordinal_position
        """)

        rows = cs.fetchall()
        logging.info(
            f"Retrieved {len(rows)} rows of schema metadata from Snowflake")

        metadata = {}
        for schema, table, column, dtype in rows:
            key = f"{schema}.{table}"
            if key not in metadata:
                metadata[key] = []
            metadata[key].append((column, dtype))

        cs.close()
        ctx.close()

        logging.info("Snowflake metadata loaded successfully")
        return metadata

    except Exception as e:
        logging.exception("Failed to retrieve Snowflake metadata")
        raise

# --------------------- Custom Metadata ---------------------


def load_custom_metadata():
    path = "langchain_agent/metadata/db_schema.yml"
    logging.info(f"Loading custom metadata from {path}")
    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        meta = {}
        for table in data.get("tables", []):
            meta[table["name"]] = [(col["name"], col.get(
                "description", "")) for col in table["columns"]]
        logging.info(f"Loaded custom metadata for {len(meta)} tables")
        return meta
    except Exception as e:
        logging.exception("Failed to load custom metadata")
        raise

# --------------------- Merge Metadata ---------------------


def merge_metadata(snowflake_meta, custom_meta):
    logging.info("Merging Snowflake and custom metadata...")
    merged = snowflake_meta.copy()
    for table, cols in custom_meta.items():
        if table in merged:
            existing_cols = set(c[0] for c in merged[table])
            new_cols = [c for c in cols if c[0] not in existing_cols]
            merged[table].extend(new_cols)
            logging.debug(
                f"Merged {len(new_cols)} columns into existing table: {table}")
        else:
            merged[table] = cols
            logging.debug(f"Added new custom table to metadata: {table}")
    logging.info("Metadata merge complete")
    return merged
