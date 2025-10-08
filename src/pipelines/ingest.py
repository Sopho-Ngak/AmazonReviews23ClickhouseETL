import json
import gzip
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterator, Dict, Any, List


import polars as pl


from config.config import logger, clickhouse_config
from src.utils.clickhouse import ClickHouseDB
from src.sql.create_schema import get_sql_query

class AmazonReviewsIngestion(ClickHouseDB):
    def __init__(self, data_folder: str="./src/data", batch_size: int = 5000):
        super().__init__()
        self.data_folder = data_folder
        self.schema = clickhouse_config['db_name']
        self.batch_size = batch_size  # number of records to process in each batch
    
    def create_table_if_not_exists(self, action_query: str) -> None:
        logger.info(f"Creating database 'amazon' if not exists")
        database_query = get_sql_query("create_database")
        self.sql_query(database_query)
        logger.info(f"Database 'amazon' is ready.")
        
        create_table_query = get_sql_query(action_query)
        
        logger.info(f"Creating table '{create_table_query['table_name']}' if not exists")
        self.sql_query(create_table_query['sql_create'])
        logger.info(f"Table '{create_table_query['table_name']}' is ready.")
        
    def read_jsonl_gz_file(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """Read compressed JSONL file and yield records"""
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    yield json.loads(line)
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            raise
        
    def _check_with_temp_table(self, batch_data: List[Dict[str, Any]], table: str) -> set:
        """Use temporary table approach for large batches"""
        # WARNING: This function is deprecated and not currently used.
        # It is kept here for reference and potential future use.
        # I switch the table design to ReplacingMergeTree to handle deduplication
        # instead of manually checking for existing records.
        
        logger.info(f"Checking existing records using stg_pivot table for batch of size {len(batch_data)}")
        try:
            # Create a temporary table name
            temp_table = f"stg_pivot"
            
            # Create temporary table
            create_temp_query = f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{temp_table} (
                    asin String,
                    user_id String,
                    parent_asin String
                ) ENGINE = Memory
            """
            
            self.sql_query(create_temp_query)
            df = pl.DataFrame(batch_data)
            df = df.select(['asin', 'user_id', 'parent_asin'])
            self.sql_write_df(df, temp_table, schema=self.schema)
            
            # free up memory
            del df
            
            # Query existing records using JOIN
            check_query = f"""
                SELECT t.asin, t.user_id, t.parent_asin 
                FROM {self.schema}.{temp_table} t
                INNER JOIN {self.schema}.{table} main ON 
                    t.asin = main.asin AND 
                    t.user_id = main.user_id AND 
                    t.parent_asin = main.parent_asin
            """
            
            result = self.sql_query(check_query)
            logger.info(f"Found {len(result)} existing records in the database.")
            existing_keys = set((row['asin'], row['user_id'], row['parent_asin']) for row in result.to_dicts())
            
            # Clean up temporary table
            self.sql_query(f"TRUNCATE TABLE {self.schema}.{temp_table}")
            
            logger.info(f"Found {len(existing_keys)} existing records in the batch.")
            del result  # free up memory
                
            return existing_keys
            
        except Exception as e:
            logger.error(f"Error in temp table check: {e}")
            raise
        
    def date_format(self, date_int: int):
        # convert "timestamp": 1598567408138 to datetime
        return datetime.fromtimestamp(date_int / 1000)
        
    def insert_batch(self, batch_data: List[Dict[str, Any]], table: str) -> int:
        """Insert a batch of records into the database"""
        logger.info(f"Inserting batch of size {len(batch_data)} into '{table}'")
        if not batch_data:
            return 0
                
        try:
            # Check for existing records to avoid duplicates
            # existing_keys = self._check_with_temp_table(batch_data, table)
            
            # Filter out existing records
            # new_records = []
            # for record in batch_data:
            #     key = (record['asin'], record['user_id'], record['parent_asin'])
            #     if key not in existing_keys:
                    # new_records.append(record)
                    
            # del existing_keys  # free up memory
                    
            # if not new_records:
            #     logger.info("No new records to insert in this batch.")
            #     return 0
            
            
            df = pl.DataFrame(batch_data)
            new_records = len(batch_data)
            del batch_data  # free up memory
            if table == 'reviews':
                # Data type conversions
                boolean_columns = ['verified_purchase']
                for col in boolean_columns:
                    if col in df.columns:
                        df = df.with_columns(pl.col(col).cast(pl.Boolean))

                if "rating" in df.columns:
                    df = df.with_columns(pl.col("rating").cast(pl.UInt8))

                if "helpful_vote" in df.columns:
                    df = df.with_columns(
                        pl.when(
                            (pl.col("helpful_vote").cast(pl.Utf8).str.strip_chars() == "") |
                            (pl.col("helpful_vote").cast(pl.Utf8).str.to_lowercase() == "null")
                        )
                        .then(None)
                        .otherwise(pl.col("helpful_vote").cast(pl.Int64))
                        .alias("helpful_vote")
                    )
    
                # timestamp conversion
                df = df.with_columns(
                    pl.col('timestamp').map_elements(
                        self.date_format, 
                        return_dtype=pl.Datetime,
                        skip_nulls=True  # optional: skip null values
                    )
                )
            
            # add ingest_ts column (referer to the date of ingestion)
            df = df.with_columns(pl.lit(datetime.now()).alias('ingest_ts'))

            self.sql_write_df(df=df, table_name=table, schema=self.schema)
            del df  # free up memory
            logger.info(f"Inserted {new_records} new records into '{table}'.")
            return new_records
            
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            raise
        
    def data_modeling(self, record: Dict[str, Any]) -> tuple:
        logger.debug(f"Modeling record with asin: {record.get('asin', 'N/A')}")
        images = record.pop('images', [])
        if not images:
            return record, None
        images: dict = images[0]
        record['text'] = record['text'].replace('<br /><br />', '\n')
        images_data = {
            'asin': record.get('asin', ''),
            'parent_asin': record.get('parent_asin', ''),
            'user_id': record.get('user_id', ''),
            'small_image_url': images['small_image_url'],
            'medium_image_url': images['medium_image_url'],
            'large_image_url': images['large_image_url'],
            'attachment_type': images['attachment_type']
        }
        logger.debug(f"Extracted image data for asin: {record.get('asin', 'N/A')}")
        return record, images_data

    def ingest_file(self, file_path: str) -> Dict[str, int]:
        """Ingest a single file"""
        logger.info(f"Starting ingestion for file: {file_path}")
        start_time = datetime.now()
        stats = {
            'total_processed': 0,
            'total_inserted': 0,
            'batches_processed': 0,
            'images_processed': 0,
            'errors': 0
        }
        reviews_table = get_sql_query("create_reviews_table")['table_name']
        images_table = get_sql_query("review_images_table")['table_name']
        
        try:
            batch_data = []
            images_data = []
            logger.info(f"Processing file: {file_path}")
            for record in self.read_jsonl_gz_file(file_path):
                try:
                    record, image_record = self.data_modeling(record)
                    if image_record:
                        images_data.append(image_record)
                        
                    batch_data.append(record)
                    stats['total_processed'] += 1
                    
                    if len(batch_data) >= self.batch_size:
                        inserted_count = self.insert_batch(batch_data, reviews_table)
                        if image_record:
                            self.insert_batch(images_data, images_table)
                            images_data = []  # Reset images batch
                            inserted_count += len(images_data)
                            stats['images_processed'] += len(images_data)
                            
                        stats['total_inserted'] += inserted_count
                        stats['batches_processed'] += 1
                        batch_data = []  # Reset batch
                        logger.info(f"Processed {stats['total_processed']} combine with image record...")
                except Exception as e:
                    logger.error(f"Error processing record: {e}")
                    stats['errors'] += 1
                    continue
                
            # Insert any remaining records
            if batch_data:
                inserted_count = self.insert_batch(batch_data, reviews_table)
                stats['total_inserted'] += inserted_count
                stats['batches_processed'] += 1
                
            if images_data:
                self.insert_batch(images_data, images_table)
                stats['images_processed'] += len(images_data)
                
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Ingestion completed for {file_path}\n")
            # logger.info(f"Stats: {stats}")
            logger.info(f"Duration: {duration} seconds\n")
            logger.info("Ingestion Stats")
            logger.info("-" * 30)
            for k, v in stats.items():
                logger.info(f"{k:20} | {v}")
            logger.info("-" * 30 + "\n")
            return stats

        except Exception as e:
            logger.error(f"Error during ingestion of file {file_path}: {e}")
            return stats
        
    def ingest_data_folder(self) -> None:
        """Ingest all files from the data folder"""
        
        data_path = Path(self.data_folder)
        if not data_path.exists():
            logger.error(f"Data folder '{self.data_folder}' does not exist.")
            sys.exit(1)
            
        logger.info(f"Starting data ingestion from folder: {self.data_folder}")
        
        # Find all .json.gz files
        files = list(data_path.rglob('*.jsonl.gz'))
        if not files:
            logger.warning(f"No .json.gz files found in '{self.data_folder}'.")
            return
        
        logger.info(f"Found {len(files)} files to ingest.")
        
        total_stats = {
            'total_processed': 0,
            'total_inserted': 0,
            'batches_processed': 0,
            'errors': 0,
            'files_processed': 0
        }
        
        for file_path in files:
            try:
                file_stats = self.ingest_file(str(file_path))
                for key in total_stats:
                    total_stats[key] += file_stats.get(key, 0)
                total_stats['files_processed'] += 1
            except Exception as e:
                logger.error(f"Error ingesting file {file_path}: {e}")
                total_stats['errors'] += 1
                continue
                    
        logger.info("Final Ingestion Stats")
        logger.info("-" * 30)
        for k, v in total_stats.items():
            logger.info(f"{k:20} | {v}")
        
        logger.info("-" * 30 + "\n")

        logger.info("\n\nData ingestion process completed.")
        
    def main(self):
        # Ensure tables exist
        self.create_table_if_not_exists("create_reviews_table")
        self.create_table_if_not_exists("review_images_table")
        # Ingest data from folder
        self.ingest_data_folder()
    
if __name__ == "__main__":
    ingestion = AmazonReviewsIngestion(batch_size=20000)
    
    ingestion.main()


    