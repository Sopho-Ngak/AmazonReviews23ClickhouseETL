from config.config import clickhouse_config

sql_queries = {
    "create_database": f"CREATE DATABASE IF NOT EXISTS {clickhouse_config['db_name']};",
    "create_reviews_table": {
        "table_name": "reviews",
        "sql_create":f"""
        CREATE TABLE IF NOT EXISTS {clickhouse_config['db_name']}.reviews
        (
            user_id String,
            parent_asin String,
            asin String,
            title String,
            text String,
            rating UInt8,
            helpful_vote Nullable(Int64),
            verified_purchase Bool,
            timestamp DateTime,
            ingest_ts DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(ingest_ts) -- Use ReplacingMergeTree for deduplication
        PARTITION BY toYear(timestamp)
        ORDER BY (asin, user_id, parent_asin);
"""},

    "review_images_table": {
        "table_name": "review_images",
        "sql_create": f"""
        CREATE TABLE IF NOT EXISTS {clickhouse_config['db_name']}.review_images
        (
            asin String,
            parent_asin String,
            user_id String,
            small_image_url String,
            medium_image_url String,
            large_image_url String,
            attachment_type String,
            ingest_ts DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(ingest_ts) -- Use ReplacingMergeTree for deduplication
        PARTITION BY toYear(ingest_ts)
        ORDER BY (asin, user_id, parent_asin);
"""
}
}

def get_sql_query(key):
    try:
        return sql_queries[key]
    except KeyError as e:
        raise e
