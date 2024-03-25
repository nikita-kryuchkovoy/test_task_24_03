"""
Module consists of STGLoader class for uploading raw data to
stg data level.
"""

import logging
from io import StringIO
from typing import Optional

import pandas as pd
import psycopg as pg
import requests

from config import TIMEOUT
from utils.decorators import with_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
h = logging.StreamHandler()
h.setLevel(logging.DEBUG)
f = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
h.setFormatter(f)
if not logger.hasHandlers():
    logger.addHandler(h)


class STGLoader:
    """Used for loading data from the source
    to the stg level in a raw format.

    Attrs:
        TIMEOUT (int): a default timeout for a requests.
    """

    TIMEOUT: int = TIMEOUT

    def __init__(self, url: str, table_name: str, schema: str):
        """Inits the STGLoader class instance.

        Args:
            url (str): target source url.
            table_name (str): target table name.
            schema (str): target schema name.
        """

        self.url = url
        self.table_name = table_name
        self.schema = schema

    def download_data(self) -> list[dict]:
        """Downloads data from the target url.

        Returns:
            list[dict]: list representation of a received data.
        """

        logger.debug(f"Downloading data from: {self.url}")
        response = requests.get(self.url, timeout=self.TIMEOUT)
        if response.ok:
            result = response.json()
            logger.debug(f"Downloaded len: {len(result)}")
            return result
        else:
            response.raise_for_status()

    @staticmethod
    def transform_data_to_df(data: list[dict]) -> pd.DataFrame:
        """Transforms data from list of dicts to pandas dataframe.

        Args:
            data (list[dict]): input list of rows.

        Returns:
            pd.DataFrame: transformed dataframe.
        """

        logger.debug(f"Transforming data from dict to df...")
        df = pd.DataFrame.from_dict(data)
        df = df.rename(columns={"userId": "user_id"})
        return df

    @with_connection
    def upload_df_to_db(
        self, df: pd.DataFrame, conn: Optional[pg.Connection] = None
    ) -> None:
        """Creates connection and uploads data into db.

        Args:
            df (pd.DataFrame): dataframe to be uploaded.
            conn (Optional[pg.Connection], optional): db connection. Defaults to None.
        """

        logger.debug(f"Uploading data to db...")
        try:
            with conn.cursor() as cursor:
                self.copy_df(
                    df=df, cursor=cursor, table_name=self.table_name, schema=self.schema
                )
        except pg.errors.UniqueViolation as e:
            logger.error(f"Data you are trying to load is already exists: {str(e)}")

    def copy_df(
        self,
        df: pd.DataFrame,
        cursor: pg.Cursor,
        table_name: str,
        columns: Optional[list[str]] = None,
        schema: Optional[str] = None,
        index: bool = False,
        block_size: int = 65_536,
    ) -> None:
        """Copy DataFrame into the database table using copy_from method.

        Args:
            df (pd.DataFrame): df to be uploaded.
            cursor (pg.Cursor): connection cursor for interacting with db.
            table_name (str): target table name.
            columns (Optional[list[str]], optional): columns to be uploaded. Defaults to None.
            schema (Optional[str], optional): target schema. Defaults to None.
            index (bool, optional): write row names (indexes) or not. Defaults to False.
            block_size (int, optional): optional copy block size. Defaults to 65_536.
        """

        buffer = StringIO()
        df.to_csv(buffer, index=index, header=False, na_rep=r"\N")
        buffer.seek(0)
        table_path = f"{schema}.{table_name}" if schema else table_name
        columns_clause = ",".join((f'"{c}"' for c in (columns or df.columns)))
        with cursor.copy(
            f"""
            COPY {table_path} ({columns_clause})
            FROM STDIN WITH (FORMAT CSV, NULL '\\N')
        """
        ) as copy:
            while data := buffer.read(block_size):
                copy.write(data)
