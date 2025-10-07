from dotenv import load_dotenv, find_dotenv
import os, sys, logging

load_dotenv(find_dotenv())

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()

logging.basicConfig(
    stream=sys.stdout, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=LOG_LEVEL)
logger = logging.getLogger(__name__)

clickhouse_config = {
    'db_type': 'clickhouse',
    'db_name': os.environ['CLICKHOUSE_DB'],
    'db_user': os.environ['CLICKHOUSE_USER'],
    'db_pass': os.environ['CLICKHOUSE_PASSWORD'],
    'db_host': os.environ['CLICKHOUSE_HOST'],
    'db_port': os.environ['CLICKHOUSE_PORT'],
    'max_chunk': 50000
}