import sys
import os
import glob
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

UTILS = os.path.join(os.path.dirname(__file__), "..", "..", "utils")
sys.path.insert(0, UTILS)

import utils_virksomhed as utils


def test_cvr_numbers__main_are_8_digits():
    folder = utils.RAW_VIRKSOMHED_FOLDER_PATH
    parquet_files = glob.glob(os.path.join(folder, "main_*.parquet"))

    for path in parquet_files:
        df = pd.read_parquet(path)
        if "cvrNummer" not in df.columns:
            continue
        invalid = df[df["cvrNummer"].astype(str).str.len() != 8]
        assert invalid.empty, (
            f"{os.path.basename(path)}: {len(invalid)} rows with invalid CVR length.\n"
            f"  Examples: {invalid['cvrNummer'].head(5).tolist()}"
        )
