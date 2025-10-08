from dbutils import Query
import polars as pl

from config.config import clickhouse_config, logger

class ClickHouseDB:
    def __init__(self):
        logger.info("Connecting to ClickHouse...")
        self.q = Query(
            db_type=clickhouse_config['db_type'],
            db=clickhouse_config['db_name'],
            db_user=clickhouse_config['db_user'],
            db_pass=clickhouse_config['db_pass'],
            db_host=clickhouse_config['db_host'],
            db_port=str(clickhouse_config['db_port'])
        )
        # check connection
        try:
            self.q.sql_query("SELECT 1 AS test")
            logger.info(f"Successfully Connected to ClickHouse database: {clickhouse_config['db_name']}")
        except Exception as e:
            logger.error("Failed to connect to ClickHouse")
            raise e
        

    def sql_query(self, sql: str) -> pl.DataFrame:
        logger.info(f"Executing SQL query")
        data = self.q.sql_query(sql=sql)
        logger.info(f"Query returned {len(data)} rows")
        return data


    def sql_write_df(self, df: pl.DataFrame, table_name: str, max_chunk: int = clickhouse_config['max_chunk'], schema: str = clickhouse_config['db_name']):
        logger.info(f"Writing DataFrame to table {table_name}...")

        if type(df) is not pl.DataFrame:
            raise ValueError("df must be a Polars DataFrame")
        
        self.q.sql_write(
            df=df,
            schema=schema,
            table_name=table_name,
            max_chunk=max_chunk
            )
        logger.info(f"Successfully wrote DataFrame to table {table_name}.")


