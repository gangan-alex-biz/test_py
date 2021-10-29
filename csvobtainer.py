"""Reading CSV file, writing its first 5 columns and 10 rows to a news csv or json file"""
from os.path import exists
from pathlib import Path
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
        if not self.is_file_cached or not self.cache:
            req = requests.get(self.url)
            with open(f"{self.cache_folder}/{self.file_name}", "wb") as file:
                file.write(req.content)

    @property
    def file_name(self) -> str:
        return self.url.split("/")[-1].split("?")[0]

    @property
    def is_file_cached(self) -> bool:
        return exists(f"{self.cache_folder}/{self.file_name}")


class FileTransformer:
    def __init__(self, file_name):
        self.file_name: str = file_name

    def read_first_cols_rows(self, cols: int, rows: int) -> pd.DataFrame:
        df = pd.read_csv(self.file_name)
        return df.iloc[:rows, :cols]


class FileSaver:
    def __init__(self, source_df, output_file_name, output_format):
        self.source_df: pd.DataFrame = source_df
        self.dest: str = f"{output_file_name[:-4]}_modified"
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
            saver()
        else:
            raise Exception("Unknown output format")


def read_settings() -> dict:
    with open("settings.yml") as f:
        settings = yaml.safe_load(f)
    return settings


def prepare_folders(settings) -> None:
    Path(settings["cache_folder"]).mkdir(parents=True, exist_ok=True)
    Path(settings["output_destination"]).mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":

    settings = read_settings()
    prepare_folders(settings)

    getter = FileGetter(settings["url"], settings["cache_folder"], settings["cache"])
    getter.get_file()

    transformer = FileTransformer(getter.file_name)
    df = transformer.read_first_cols_rows(
        cols=settings["cols"],
        rows=settings["rows"],
    )

    output_file_name = f"{settings['output_destination']}/{getter.file_name}"

    saver = FileSaver(df, output_file_name, settings["output_format"])
    saver.save()
