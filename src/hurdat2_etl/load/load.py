"""Data Load Module"""

import os

from ..config.settings import Settings


def load_data(data: str) -> None:
    """Load data to target"""
    target_path = Settings.TARGET_PATH
    os.makedirs(target_path, exist_ok=True)

    with open(os.path.join(target_path, "processed_data.txt"), "w") as file:
        file.write(data)
