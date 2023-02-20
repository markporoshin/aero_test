import logging
import pandas as pd
from sqlalchemy import create_engine


class PostgresLoader:

    def __init__(self,
                 connection_config: dict,
                 table: str,
                 schema: str,
                 chunksize: int = 10000,
                 replication_type: str = 'replace'):
        self.logger = logging.getLogger(__name__)
        self.table = table
        self.schema = schema
        self.connection_config = connection_config
        self.replication_type = replication_type
        self.chunksize = chunksize

    def get_engine(self):
        conn_string = f"postgresql+psycopg2://{self.connection_config['user']}:{self.connection_config['password']}@" \
                      f"{self.connection_config['host']}:{self.connection_config['port']}/" \
                      f"{self.connection_config['dbname']}"

        self.logger.info(conn_string)

        if 'ssl' in self.connection_config and self.connection_config['ssl'] == 'true':
            return create_engine(conn_string, connect_args={'sslmode':'require'})

        return create_engine(conn_string)

    def load(self, data_df: pd.DataFrame):
        if data_df is not None and not data_df.empty:
            pg_engine = self.get_engine()
            data_df.to_sql(
                name=self.table,
                con=pg_engine,
                schema=self.schema,
                method='multi',
                if_exists=self.replication_type,
                chunksize=self.chunksize
            )
            self.logger.info(f"Loaded {data_df.shape[0]} rows")
        else:
            self.logger.info("DataFrame is None or empty")
