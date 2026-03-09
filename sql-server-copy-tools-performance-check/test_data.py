import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta

import config


def make_test_df(n_rows):
    rng = np.random.default_rng(seed=42)
    categories = [f"cat_{i}" for i in range(20)]
    base_time = datetime(2024, 1, 1)

    df = pd.DataFrame({
        "id": np.arange(1, n_rows + 1, dtype=np.int32),
        "value": rng.standard_normal(n_rows).astype(np.float64),
        "amount": rng.integers(1, 10000, size=n_rows, dtype=np.int32),
        "category": rng.choice(categories, size=n_rows),
        "description": [f"item_{i}_" + "x" * rng.integers(10, 80) for i in range(n_rows)],
        "created_at": [base_time + timedelta(seconds=int(s)) for s in rng.integers(0, 86400 * 365, size=n_rows)],
    })
    return df


def save_to_csv(df, filename="bulk_data.csv"):
    host_path = os.path.join(config.STAGING_DIR_HOST, filename)
    df.to_csv(host_path, index=False, sep=",")
    container_path = f"{config.STAGING_DIR_CONTAINER}/{filename}"
    return host_path, container_path


if __name__ == "__main__":
    df = make_test_df(10)
    print(df)
    print(f"\ndtypes:\n{df.dtypes}")
