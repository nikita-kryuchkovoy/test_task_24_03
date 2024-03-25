"""
Module consists of DDSLoader class for interacting
with stg and dds data levels (for loading data from stg to dds.)
"""

import asyncio
import hashlib
import logging
from io import StringIO
from typing import Optional

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from config import CONN_STR
from utils.decorators import with_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
h = logging.StreamHandler()
h.setLevel(logging.DEBUG)
f = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
h.setFormatter(f)
if not logger.hasHandlers():
    logger.addHandler(h)


class DDSLoader:
    """Used for loading raw data from STG to DDS.
    Data model: Data Vault 2.0.
    """

    def __init__(
        self,
        table_h_users: str,
        table_h_letters: str,
        table_s_letters: str,
        table_l_posts: str,
        schema: str,
    ):
        """Inits the DDSLoader class instance.

        Args:
            table_h_users (str): table name of hub 'users'.
            table_h_letters (str): table name of hub 'letters'.
            table_s_letters (str): table name of satellite 'letters'.
            table_l_posts (str): table name of link 'posts'.
            schema (str): target dds schema name.
        """

        self.table_h_users = table_h_users
        self.table_h_letters = table_h_letters
        self.table_s_letters = table_s_letters
        self.table_l_posts = table_l_posts
        self.schema = schema

    @with_connection
    def download_stg_data(
        self, conn: Optional[psycopg.Connection] = None
    ) -> list[dict]:
        """Downloads raw data from the stg level.

        Args:
            conn (psycopg.Connection, optional): db connection. Defaults to None.

        Returns:
            list[dict]: a list of dicts representing rows from the table.
        """

        logger.debug(f"Downloading data from raw_test_data table...")
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                    SELECT 
                        user_id,
                        id,
                        title,
                        body
                    FROM stg.raw_test_data;

                """
            )
            data = cursor.fetchall()
        return data

    @staticmethod
    def transform_data_to_df(data: list[dict]) -> pd.DataFrame:
        """Transforms list of dicts into dataframe.

        Args:
            data (list[dict]): list representing table.

        Returns:
            pd.DataFrame: pandas df format.
        """

        logger.debug(f"Transforming data to df...")
        df = pd.DataFrame.from_dict(data)
        return df

    @staticmethod
    def add_hashes_to_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        """Adds hashes made of business keys.

        Args:
            df (pd.DataFrame): common raw version of data.

        Returns:
            pd.DataFrame: updated df.
        """

        logger.debug(f"Adding hashes: user_id_hash, letter_id_hash to df...")
        df["user_id_hash"] = df["user_id"].apply(
            lambda x: hashlib.md5(str(x).encode()).hexdigest()
        )
        df["letter_id_hash"] = df["id"].apply(
            lambda x: hashlib.md5(str(x).encode()).hexdigest()
        )
        return df

    @staticmethod
    def split_df_to_tables(df: pd.DataFrame) -> tuple[pd.DataFrame]:
        """Splits common dataframe into target dds tables.

        Args:
            df (pd.DataFrame): updated version of data.

        Returns:
            tuple[pd.DataFrame]: a tuple with target dataframes to be uploaded.
        """

        logger.debug(f"Splitting common df to target dataframes...")
        df_users_hub = df[["user_id", "user_id_hash"]]
        df_users_hub = df_users_hub.drop_duplicates()
        df_letters_hub = df[["id", "letter_id_hash"]]
        df_letters_hub = df_letters_hub.rename(columns={"id": "letter_id"})
        df_letters_satellite = df[["letter_id_hash", "title", "body"]]
        df_posts_link = df[["user_id_hash", "letter_id_hash"]]
        return df_users_hub, df_letters_hub, df_letters_satellite, df_posts_link

    @staticmethod
    async def upload_dds_table(
        df: pd.DataFrame,
        table_name: str,
        columns: Optional[list[str]] = None,
        schema: Optional[str] = None,
        index: bool = False,
        block_size: int = 65_536,
    ) -> None:
        """Asynchronously uploads data into target table using copy_from method.

        Args:
            df (pd.DataFrame): df to be uploaded.
            table_name (str): target table name.
            columns (Optional[list[str]], optional): columns to be uploaded. Defaults to None.
            schema (Optional[str], optional): target schema name. Defaults to None.
            index (bool, optional): write row names (indexes) or not. Defaults to False.
            block_size (int, optional): optional copy block size. Defaults to 65_536.
        """

        try:
            aconn = await psycopg.AsyncConnection.connect(CONN_STR)
            async with aconn:
                async with aconn.cursor() as cur:
                    buffer = StringIO()
                    df.to_csv(buffer, index=index, header=False, na_rep=r"\N")
                    buffer.seek(0)
                    table_path = f"{schema}.{table_name}" if schema else table_name
                    columns_clause = ",".join(
                        (f'"{c}"' for c in (columns or df.columns))
                    )
                    logger.debug(f"Uploading df into table: {schema}.{table_name}...")
                    async with cur.copy(
                        f"""
                        COPY {table_path} ({columns_clause})
                        FROM STDIN WITH (FORMAT CSV, NULL '\\N')
                    """
                    ) as copy:
                        while data := buffer.read(block_size):
                            await copy.write(data)
        except psycopg.errors.UniqueViolation as e:
            logger.error(f"Data you are trying to load is already exists: {str(e)}")

    async def upload_dds_data(
        self,
        df_users_hub: pd.DataFrame,
        df_letters_hub: pd.DataFrame,
        df_letters_satellite: pd.DataFrame,
        df_posts_link: pd.DataFrame,
    ) -> None:
        """The main method for uploading all dds data.

        Args:
            df_users_hub (pd.DataFrame): dataframe of table-hub 'users'.
            df_letters_hub (pd.DataFrame): dataframe of table-hub 'letters'.
            df_letters_satellite (pd.DataFrame): dataframe of table-satellite 'letters'.
            df_posts_link (pd.DataFrame): dataframe of table-link 'posts'.
        """

        task_users_hub = asyncio.create_task(
            self.upload_dds_table(
                df=df_users_hub, table_name=self.table_h_users, schema=self.schema
            )
        )
        task_letters_hub = asyncio.create_task(
            self.upload_dds_table(
                df=df_letters_hub, table_name=self.table_h_letters, schema=self.schema
            )
        )
        task_letters_satellilte = asyncio.create_task(
            self.upload_dds_table(
                df=df_letters_satellite,
                table_name=self.table_s_letters,
                schema=self.schema,
            )
        )
        task_posts_link = asyncio.create_task(
            self.upload_dds_table(
                df=df_posts_link, table_name=self.table_l_posts, schema=self.schema
            )
        )

        await task_users_hub
        await task_letters_hub
        await task_letters_satellilte
        await task_posts_link
