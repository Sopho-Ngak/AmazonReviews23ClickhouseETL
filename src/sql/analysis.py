from config.config import clickhouse_config

queries = {
    'total_reviews': f"SELECT COUNT(*) as count FROM {clickhouse_config['db_name']}.reviews",
    'unique_products': f"SELECT COUNT(DISTINCT asin) as count FROM {clickhouse_config['db_name']}.reviews",
    'unique_users': f"SELECT COUNT(DISTINCT user_id) as count FROM {clickhouse_config['db_name']}.reviews",
    'date_range': f"SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date FROM {clickhouse_config['db_name']}.reviews",
    'rating_distribution': f"""
        SELECT
            rating,
            COUNT(*) as count,
            COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {clickhouse_config['db_name']}.reviews) as percentage
        FROM {clickhouse_config['db_name']}.reviews
        GROUP BY rating
        ORDER BY rating;
        """,
    "verified_vs_unverified": f"""
        SELECT
            verified_purchase,
            COUNT(*) AS count,
            AVG(rating) AS avg_rating
        FROM {clickhouse_config['db_name']}.reviews
        GROUP BY verified_purchase;
        """,
    "product_popularity": f"""
        SELECT 
            asin,
            COUNT(*) as review_count,
            AVG(rating) as avg_rating,
            MIN(rating) as min_rating,
            MAX(rating) as max_rating,
            SUM(
                CASE
                    WHEN helpful_vote > 0 THEN helpful_vote
                    ELSE 0
                END
                ) as total_helpful_votes,
            SUM(
                CASE
                    WHEN helpful_vote < 0 THEN helpful_vote
                    ELSE 0
                END
                ) as total_negative_votes,
            COUNT(DISTINCT user_id) as unique_reviewers
        FROM {clickhouse_config['db_name']}.reviews 
        GROUP BY asin
        HAVING review_count >= 5
        ORDER BY review_count DESC
        LIMIT 100;
        """,
    "temporal_trends": f"""
        SELECT 
            toYear(toDateTime(timestamp)) as year,
            toMonth(toDateTime(timestamp)) as month,
            COUNT(*) as review_count,
            AVG(rating) as avg_rating,
            COUNT(DISTINCT asin) as unique_products,
            COUNT(DISTINCT user_id) as unique_users
        FROM {clickhouse_config['db_name']}.reviews
        WHERE timestamp IS NOT NULL
        GROUP BY year, month
        ORDER BY year, month
        LIMIT 1000;
        """,
    "user_behavior": f"""
        SELECT 
            user_id,
            COUNT(*) as total_reviews,
            AVG(rating) as avg_rating,
            MIN(rating) as min_rating,
            MAX(rating) as max_rating,
            COUNT(DISTINCT asin) as unique_products,
            SUM(
                CASE
                    WHEN helpful_vote > 0 THEN helpful_vote
                    ELSE 0
                END
                ) as total_helpful_votes,
            SUM(
                CASE
                    WHEN helpful_vote < 0 THEN helpful_vote
                    ELSE 0
                END
                ) as total_negative_votes,
            SUM(CASE WHEN verified_purchase = true THEN 1 ELSE 0 END) as verified_purchases
        FROM {clickhouse_config['db_name']}.reviews
        GROUP BY user_id
        HAVING total_reviews >= 3
        ORDER BY total_reviews DESC
        LIMIT 1000
        """
}
