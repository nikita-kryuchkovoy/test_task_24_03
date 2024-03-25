import logging

from config import TARGET_STG_SCHEMA_NAME, TARGET_STG_TABLE_NAME, TARGET_URL
from stg_layer.stg_loader import STGLoader

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
h = logging.StreamHandler()
h.setLevel(logging.DEBUG)
f = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
h.setFormatter(f)
if not logger.hasHandlers():
    logger.addHandler(h)

if __name__ == "__main__":
    logger.debug("Started loading stg data...")
    stg_loader = STGLoader(TARGET_URL, TARGET_STG_TABLE_NAME, TARGET_STG_SCHEMA_NAME)
    downloaded_data = stg_loader.download_data()
    df = stg_loader.transform_data_to_df(downloaded_data)
    stg_loader.upload_df_to_db(df=df)
