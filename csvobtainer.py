"""
Reading CSV file, writing its first N columns and M rows to a new csv or json file.
All the config is placed into settings.yml file.
"""
import functools
import logging
import os
from typing import Dict

import pandas as pd
import requests
import yaml


def log_info(func):
    """Logs function's docstring and arguments each time it is called"""
    @functools.wraps(func)
    def wrapped(*args, ** kwargs):
        res = None
        try:
            logging.info(func.__doc__.split("\n")[0])
            res = func(*args, ** kwargs)
        except:
            logging.error("Exception occurred", exc_info=True)
        return res
    return wrapped


class FileGetter:
    def __init__(self, url, cache_folder, cache):
        self.url: str = url
        self.cache_folder: str = cache_folder
        self.cache: bool = cache

    @log_info
    def get_file(self) -> None:
        """Defining whether new or fresh file should be downloaded"""
        if not self.cache:
            logging.warning("Caching source file is disabled in the settings")
        if not self.cache or not self.is_file_cached:
            self.download_and_save_file()

    @log_info
    def download_and_save_file(self):
        """Downloading a file"""
        req = requests.get(self.url)
        with open(self.file_path, "wb") as file:
            file.write(req.content)

    @property
    @log_info
    def is_file_cached(self) -> bool:
        """Looking for existing file"""
        return os.path.exists(self.file_name)

    @property
    def file_path(self):
        return os.path.join(self.cache_folder, self.file_name)

    @property
    def file_name(self) -> str:
        return self.url.split('/')[-1].split('?')[0]


class FileTransformer:
    def __init__(self, file_path):
        self.file_path: str = file_path

    @log_info
    def read_first_cols_rows(self, cols: int, rows: int) -> pd.DataFrame:
        """Extracting first rows and columns of a table according to args provided"""
        df = pd.read_csv(self.file_path)
        return df.iloc[:rows, :cols]


class FileSaver:
    def __init__(self, source_df, output_folder, output_file_name, output_format):
        self.source_df: pd.DataFrame = source_df
        self.dest: str = os.path.join(
            output_folder,
            f"{output_file_name[:-4]}_modified.{output_format}"
        )
        self.output_format: str = output_format
        self.formats: Dict[str] = {
            "json": self.save_as_json,
            "csv": self.save_as_csv,
        }

    @log_info
    def save_as_csv(self) -> None:
        """Saving the file in csv format"""
        self.source_df.to_csv(self.dest)

    @log_info
    def save_as_json(self) -> None:
        """Saving the file in json format"""
        self.source_df.to_json(self.dest, orient="records")

    def save(self) -> None:
        """Defines and runs saver function"""
        saver = self.formats.get(self.output_format, None)
        if saver:
            saver()
            logging.info(f"File path: {self.dest}")
        else:
            logging.exception("Exception occurred: Unknown output format")


def read_settings() -> dict:
    """Reading setting from yml file"""
    with open("settings.yml") as f:
        settings = yaml.safe_load(f)
    return settings


def prepare_folder(folder: str) -> None:
    """Creating a folder if it does not exist"""
    if folder and not os.path.exists(folder):
        os.makedirs(folder)


def main():

    settings = read_settings()

    for folder in ["cache_folder", "output_destination", "logs_folder"]:
        prepare_folder(settings[folder])

    logging.basicConfig(
        filename=os.path.join(settings['logs_folder'], "log.txt"),
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )

    logging.info("Settings used:")
    for k, v in settings.items():
        logging.info(f"{k}:{v}")

    logging.info("--Script started--")

    getter = FileGetter(
        settings["url"],
        settings["cache_folder"],
        settings["cache"],
    )
    getter.get_file()

    transformer = FileTransformer(getter.file_path)
    df = transformer.read_first_cols_rows(
        cols=settings["cols"],
        rows=settings["rows"],
    )

    saver = FileSaver(
        df,
        settings["output_destination"],
        getter.file_name,
        settings["output_format"],
    )
    saver.save()

    logging.info("--Script ended --")


if __name__ == "__main__":
    main()
