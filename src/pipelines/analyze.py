from pathlib import Path
import polars as pl
import matplotlib.pyplot as plt


from src.utils.clickhouse import ClickHouseDB
from config.config import logger
from src.sql.analysis import queries as analysis_sql_queries

class AmazonReviewsAnalysis(ClickHouseDB):
    """Handles analysis of Amazon reviews data using Polars"""

    def __init__(self, output_dir: str = "./src/data/analysis_output"):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def basic_data_overview(self) -> dict[str, any]:
        """Get basic overview of the dataset"""
        logger.info("Starting basic data overview analysis...")
        overview = {}
        
        for query in ['rating_distribution', 'total_reviews', 'unique_products', 'unique_users', 'date_range', 'verified_vs_unverified']:
            try:
                df = self.sql_query(analysis_sql_queries[query])
                overview[query] = df.to_dicts()
                logger.info(f"Fetched {query}: {len(df)} records")
            except Exception as e:
                logger.error(f"Error occurred while executing query '{query}': {e}")
                overview[query] = None
        return overview
    
    def analyze_product_popularity(self) -> pl.DataFrame:
        """Analyze product popularity and ratings"""
        logger.info("Starting product popularity analysis...")
        try:
            df = self.sql_query(analysis_sql_queries['product_popularity'])
            logger.info(f"Fetched product popularity data: {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error occurred while executing product popularity query: {e}")
            return pl.DataFrame()
        
    def analyze_temporal_trends(self) -> pl.DataFrame:
        """Analyze temporal trends in reviews"""
        logger.info("Analyzing temporal trends...")
        
        try:
            df = self.sql_query(analysis_sql_queries['temporal_trends'])
            # Create date column
            df = df.with_columns([
                pl.date(pl.col("year"), pl.col("month"), 1).alias("date")
            ])
            logger.info(f"Fetched temporal trends data: {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error occurred while executing temporal trends query: {e}")
            return pl.DataFrame()
        
    def analyze_user_behavior(self) -> pl.DataFrame:
        """Analyze user review behavior"""
        logger.info("Analyzing user behavior...")
        
        try:
            df = self.sql_query(analysis_sql_queries['user_behavior'])
            # Calculate metrics
            df = df.with_columns([
                (pl.col("verified_purchases") / pl.col("total_reviews")).alias("verified_ratio"),
                (pl.col("total_helpful_votes") / pl.when(pl.col("total_negative_votes") > 0)
                .then(pl.col("total_helpful_votes")).otherwise(1)).alias("helpfulness_ratio")
            ])
            logger.info(f"Fetched user behavior data: {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error occurred while executing user behavior query: {e}")
            return pl.DataFrame()
        
    def create_visualizations(self, data_dict: dict):
        """Create visualizations from analyzed data"""
        logger.info("Creating visualizations...")
        
        plt.style.use('seaborn-v0_8')
        fig_size = (12, 8)
        
        # 1. Rating Distribution
        if 'rating_distribution' in data_dict and data_dict['rating_distribution']:
            rating_data = pl.from_records(data_dict['rating_distribution'])

            logger.info("Creating rating distribution plot...")
            
            fig, ax = plt.subplots(figsize=fig_size)
            ax.bar(rating_data['rating'], rating_data['count'])
            ax.set_xlabel('Rating')
            ax.set_ylabel('Number of Reviews')
            ax.set_title('Distribution of Review Ratings')
            plt.tight_layout()
            plt.savefig(self.output_dir / 'rating_distribution.png', dpi=300)
            plt.close()
            
        # 2. Product Popularity Analysis
        if 'product_popularity' in data_dict:
            prod_df: pl.DataFrame = data_dict['product_popularity']
            
            # Top products by review count
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            top_20 = prod_df.head(20)
            ax1.barh(range(len(top_20)), top_20['review_count'])
            ax1.set_xlabel('Number of Reviews')
            ax1.set_title('Top 20 Products by Review Count')
            ax1.set_yticks(range(len(top_20)))
            ax1.set_yticklabels(top_20['asin'].to_list())
            
            # Rating vs Review Count scatter
            sample_df = prod_df.sample(n=min(1000, len(prod_df)))
            ax2.scatter(sample_df['review_count'], sample_df['avg_rating'], alpha=0.6)
            ax2.set_xlabel('Number of Reviews')
            ax2.set_ylabel('Average Rating')
            ax2.set_title('Average Rating vs Review Count')
            
            plt.tight_layout()
            plt.savefig(self.output_dir / 'product_analysis.png', dpi=300)
            plt.close()
            
        # 3. Temporal Trends
        if 'temporal_trends' in data_dict:
            temp_df = data_dict['temporal_trends']
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
            
            # Review count over time
            ax1.plot(temp_df['date'], temp_df['review_count'])
            ax1.set_xlabel('Date')
            ax1.set_ylabel('Number of Reviews')
            ax1.set_title('Review Count Over Time')
            ax1.tick_params(axis='x', rotation=45)
            
            # Average rating over time
            ax2.plot(temp_df['date'], temp_df['avg_rating'], color='red')
            ax2.set_xlabel('Date')
            ax2.set_ylabel('Average Rating')
            ax2.set_title('Average Rating Over Time')
            ax2.tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            plt.savefig(self.output_dir / 'temporal_trends.png', dpi=300)
            plt.close()
            
        # 4. User Behavior Analysis
        if 'user_behavior' in data_dict:
            user_df = data_dict['user_behavior']
            
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
            
            # Distribution of reviews per user
            ax1.hist(user_df['total_reviews'], bins=30, edgecolor='black')
            ax1.set_xlabel('Number of Reviews per User')
            ax1.set_ylabel('Frequency')
            ax1.set_title('Distribution of Reviews per User')
            ax1.set_yscale('log')
            
            # Average rating distribution
            ax2.hist(user_df['avg_rating'], bins=20, edgecolor='black')
            ax2.set_xlabel('Average Rating per User')
            ax2.set_ylabel('Frequency')
            ax2.set_title('Distribution of Average Ratings per User')
            
            # Verified purchase ratio
            ax3.hist(user_df['verified_ratio'], bins=20, edgecolor='black')
            ax3.set_xlabel('Verified Purchase Ratio')
            ax3.set_ylabel('Frequency')
            ax3.set_title('Distribution of Verified Purchase Ratios')
            
            # Helpfulness vs Activity
            ax4.scatter(user_df['total_reviews'], user_df['helpfulness_ratio'], alpha=0.6)
            ax4.set_xlabel('Total Reviews')
            ax4.set_ylabel('Helpfulness Ratio')
            ax4.set_title('User Activity vs Helpfulness')
            
            plt.tight_layout()
            plt.savefig(self.output_dir / 'user_behavior.png', dpi=300)
            plt.close()
        
        logger.info(f"Visualizations saved to {self.output_dir}")
            
    def main(self) -> None:
        data_dict = {}
        
        # Basic overview
        logger.info("Step 1: Basic data overview")
        data_dict['basic_overview'] = self.basic_data_overview()
        
        # Product analysis
        logger.info("Step 2: Product popularity analysis")
        data_dict['product_popularity'] = self.analyze_product_popularity()
        
        # Temporal trends
        logger.info("Step 3: Temporal trends analysis")
        data_dict['temporal_trends'] = self.analyze_temporal_trends()
        
        # User behavior
        logger.info("Step 4: User behavior analysis")
        data_dict['user_behavior'] = self.analyze_user_behavior()
        
        # Extract rating distribution for visualizations
        if 'basic_overview' in data_dict and data_dict['basic_overview'].get('rating_distribution'):
            data_dict['rating_distribution'] = data_dict['basic_overview']['rating_distribution']
        
        # create visual
        self.create_visualizations(data_dict)
        
        logger.info("Analysis completed successfully!")
        logger.info("\n" + "="*60)
        logger.info("ANALYSIS COMPLETE - KEY INSIGHTS:")
        logger.info("="*60)
        # print(insights)
        logger.info("\n" + "="*60)
        logger.info(f"Detailed results and visualizations saved to: {self.output_dir}")
        logger.info("="*60)

if __name__ == "__main__":
    analysis = AmazonReviewsAnalysis()
    analysis.main()
