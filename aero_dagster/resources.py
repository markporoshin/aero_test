import os
from dotenv import load_dotenv
from dagster import resource, get_dagster_logger

from connector import RestExtractor, PostgresLoader

load_dotenv()


@resource
def rest_extractor():
    extractor = RestExtractor(
        url=os.getenv('SRC_REST_URL', 'https://random-data-api.com/api/cannabis/random_cannabis?size=10')
    )
    extractor.logger = get_dagster_logger()
    return extractor


@resource
def postgres_loader():
    loader = PostgresLoader(
        connection_config={
            'host': os.getenv('DST_HOST'),
            'port': os.getenv('DST_PORT'),
            'dbname':  os.getenv('DST_DBNAME'),
            'user':  os.getenv('DST_USER'),
            'password':  os.getenv('DST_PASSWORD'),
        },
        table=os.getenv('DST_TARGET_TABLE', 'test_table'),
        schema=os.getenv('DST_TARGET_SCHEMA', 'test_schema'),
    )
    loader.logger = get_dagster_logger()
    return loader
