from urllib.error import HTTPError

import logging
import pandas as pd


class RestExtractor:

    def __init__(self, url):
        self.logger = logging.getLogger(__name__)
        self.path = url

    def extract(self) -> pd.DataFrame:
        try:
            df = pd.read_json(self.path)
            self.logger.info(f"Extracted {df.shape[0]} rows")
            return df
        except HTTPError as e:
            pass
