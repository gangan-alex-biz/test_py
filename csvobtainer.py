"""Reading CSV file, writing its first 5 columns and 10 rows to a news csv or json file"""
from os.path import exists
from typing import Dict

import pandas as pd
import requests

URL = "https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2020-financial-year-provisional/Download-data/annual-enterprise-survey-2020-financial-year-provisional-size-bands-csv.csv"
cache_folder = "initial_files"


class FileGetter:
    def __init__(self, url):
        self.url: str = url

    def get_file(self) -> None:
        if not self.is_file_cached:
            req = requests.get(self.url)
            with open(self.file_name, "wb") as file:
                file.write(req.content)

    @property
    def file_name(self) -> str:
        return self.url.split("/")[-1].split("?")[0]

    @property
    def is_file_cached(self) -> bool:
        return exists(self.file_name)


class FileTransformer:
    def __init__(self, file_name):
        self.file_name: str = file_name

    def read_first_cols_rows(self, cols, rows):
        df = pd.read_csv(self.file_name)
        return df.iloc[:rows, :cols]


class FileSaver:
    def __init__(self, source, output_file_name, output_format):
        self.source: pd.DataFrame = source
        self.dest: str = output_file_name
        self.output_format: str = output_format
        self.formats: Dict[str] = {
            "json": self.save_as_json,
            "csv": self.save_as_csv,
        }

    def save_as_csv(self) -> None:
        self.source.to_csv(f"{self.dest}.csv")

    def save_as_json(self) -> None:
        self.source.to_json(f"{self.dest}.json", orient="records")

    def save(self) -> None:
        saver = self.formats.get(self.output_format, None)
        if saver:
            saver()
        else:
            raise Exception("Unknown output format")


if __name__ == "__main__":
    getter = FileGetter(URL)
    getter.get_file()

    transformer = FileTransformer(getter.file_name)
    df = transformer.read_first_cols_rows(cols=5, rows=10)

    saver = FileSaver(df, "modified_file", "csv")
    saver.save()
