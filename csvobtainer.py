"""
Reading CSV file, writing its first 5 columns and 10 rows to a news csv or json file.
All the config is placed into settings.yml file.
"""
import logging
import os
from typing import Dict

import pandas as pd
import requests
import yaml


class FileGetter:
    def __init__(self, url, cache_folder, cache):
        self.url: str = url
        self.cache_folder: str = cache_folder
        self.cache: bool = cache

    def get_file(self) -> None:
        if not self.cache:
            logging.warning("Caching source file is disabled in the settings")
        if not self.is_file_cached or not self.cache:
            try:
                req = requests.get(self.url)
                with open(self.file_name, "wb") as file:
                    file.write(req.content)
                logging.info("Downloaded fresh copy of the source file")
            except:
                logging.error("Exception occurred", exc_info=True)
        else:
            logging.info("Using previously downloaded file")

    @property
    def file_name(self) -> str:
        return f"{self.cache_folder}/{self.url.split('/')[-1].split('?')[0]}"

    @property
    def is_file_cached(self) -> bool:
        return os.path.exists(self.file_name)


class FileTransformer:
    def __init__(self, file_name):
        self.file_name: str = file_name

    def read_first_cols_rows(self, cols: int, rows: int) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.file_name)
        except:
            logging.error("Exception occurred", exc_info=True)
        logging.info(f"Extracted {cols} columns and {rows} rows")
        return df.iloc[:rows, :cols]


class FileSaver:
    def __init__(self, source_df, output_folder, output_file_name, output_format):
        self.source_df: pd.DataFrame = source_df
        self.dest: str = f"{output_folder}/{output_file_name[:-4]}_modified"
        self.output_format: str = output_format
        self.formats: Dict[str] = {
            "json": self.save_as_json,
            "csv": self.save_as_csv,
        }

    def save_as_csv(self) -> None:
        self.source_df.to_csv(f"{self.dest}.csv")

    def save_as_json(self) -> None:
        self.source_df.to_json(f"{self.dest}.json", orient="records")

    def save(self) -> None:
        saver = self.formats.get(self.output_format, None)
        if saver:
            try:
                saver()
            except:
                logging.info(f"Saved to {self.dest}.{self.output_format} file")
        else:
            logging.exception("Exception occurred: Unknown output format")


def read_settings() -> dict:
    with open("settings.yml") as f:
        settings = yaml.safe_load(f)
    return settings


def prepare_folder(folder: str) -> None:
    if folder and not os.path.exists(folder):
        os.makedirs(folder)


if __name__ == "__main__":

    settings = read_settings()
    for folder in ["cache_folder", "output_destination", "logs_folder"]:
        prepare_folder(settings[folder])

    logging.basicConfig(
        filename=f"{settings['logs_folder']}/log.txt",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )

    logging.info("Script started")

    getter = FileGetter(settings["url"], settings["cache_folder"], settings["cache"])
    getter.get_file()

    transformer = FileTransformer(getter.file_name)
    df = transformer.read_first_cols_rows(
        cols=settings["cols"],
        rows=settings["rows"],
    )

    saver = FileSaver(
        df, settings["output_destination"], getter.file_name, settings["output_format"]
    )
    saver.save()
