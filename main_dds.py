import asyncio
import logging

from config import (
    TARGET_DDS_SCHEMA_NAME,
    TARGET_DDS_TABLE_HUB_LETTERS,
    TARGET_DDS_TABLE_HUB_USERS,
    TARGET_DDS_TABLE_LINK_POSTS,
    TARGET_DDS_TABLE_SATELLITE_LETTERS,
)
from dds_layer.dds_loader import DDSLoader

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
h = logging.StreamHandler()
h.setLevel(logging.DEBUG)
f = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
h.setFormatter(f)
if not logger.hasHandlers():
    logger.addHandler(h)

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    dss_loader = DDSLoader(
        table_h_users=TARGET_DDS_TABLE_HUB_USERS,
        table_h_letters=TARGET_DDS_TABLE_HUB_LETTERS,
        table_s_letters=TARGET_DDS_TABLE_SATELLITE_LETTERS,
        table_l_posts=TARGET_DDS_TABLE_LINK_POSTS,
        schema=TARGET_DDS_SCHEMA_NAME,
    )
    logger.debug("Started loading dds data...")
    data = dss_loader.download_stg_data()
    df = dss_loader.transform_data_to_df(data)
    df = dss_loader.add_hashes_to_raw_data(df)
    df_users_hub, df_letters_hub, df_letters_satellite, df_posts_link = (
        dss_loader.split_df_to_tables(df)
    )
    async_upload = dss_loader.upload_dds_data(
        df_users_hub, df_letters_hub, df_letters_satellite, df_posts_link
    )
    asyncio.run(async_upload)
