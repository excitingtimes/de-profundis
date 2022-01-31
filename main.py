#!/usr/bin/env python
# -*- coding: utf-8 -*-



# -----------------------
# -------------------------------------------------
# ------------------------------------------------------------------------------------------------
# De profundiS : Datasets Sycnhronizer utility
# ------------------------------------------------------------------------------------------------
# -------------------------------------------------
# -----------------------



# ---
# Standard library imports
# ---
import os
import logging
import datetime
import uuid
import argparse
import concurrent.futures
import requests
import pandas as pd
from time import time
from time import sleep



# ---
# Pretty-printing with Rich
# ---
from rich import print
from rich import pretty
from rich import inspect
from rich.console import Console
from rich.theme import Theme
from rich.traceback import install
from rich.progress import Progress
from rich.logging import RichHandler
from rich.table import Table
pretty.install() # Pretty printing and highlighting of data structures (REPL)
# inspect(my_list, methods=True)
custom_theme = Theme({
    "quote": "underline italic magenta",
    "title": "underline italic bold",
    "warning": "dim orange3",
    "warning2": "orange3",
    "danger": "bold red",
})
console = Console(
    theme=custom_theme,
)
install(show_locals=False)
FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("rich")
logging.disable(level=logging.DEBUG)
trim = lambda s, max_len: s if len(s) <= max_len else s[:max_len-3] + '...'
STATUSES = {
    'downloaded': '✅',
    'warning': '🟠',
    'error': '❌',
}



def download_file_from_google_drive(id, destination):
    def get_confirm_token(response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value

        return None

    def save_response_content(response, destination):
        CHUNK_SIZE = 32768

        with open(destination, "wb") as f:
            for chunk in response.iter_content(CHUNK_SIZE):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)

    URL = f"https://docs.google.com/spreadsheet/ccc?key={id}&output=csv"

    session = requests.Session()

    response = session.get(URL, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)    



INCLUDE = []
EXCLUDE = []
DELETE_ZIP = True
UNZIP = True
ROOT = "/home/mila/s/sonnery.hugo/scratch/"
DO_ERASE = False
DO_UNZIP = False
DO_DELETE_ZIP = False
SPREADSHEET_ID = "10ftkEU-FQsGCrrUW4lSuip0g5joUWD_04EJtUeywrLM"
SPREADSHEET_FILENAME = "datasets_index.csv"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="De profundiS : Datasets Sycnhronizer utility",
    )


    group_parser = parser.add_argument_group("Download-related options")
    group_parser.add_argument(
        "--include", 
        help="Specific datasets to exclude",
        type=list,
        default=INCLUDE,
    )
    group_parser.add_argument(
        "--exclude", 
        help="Specific datasets to exclude",
        type=list,
        default=EXCLUDE,
    )
    group_parser.add_argument(
        "--projects", 
        help="Specific project to exclude",
        type=list,
        default=INCLUDE,
    )
    group_parser.add_argument(
        "--delete_zip", 
        help="Whether to delete the zip/tar/... files after downloading",
        type=str,
        default=DELETE_ZIP,
    )
    group_parser.add_argument(
        "--unzip", 
        help="Whether to unzip the downloaded files",
        type=str,
        default=UNZIP,
    )


    group_parser = parser.add_argument_group("Path-related options")
    group_parser.add_argument(
        "--root", 
        help="Location of the directory containing the ./datasets/ folder",
        type=str,
        default=ROOT,
    )


    group_parser = parser.add_argument_group("Actions")
    group_parser.add_argument(
        "--do_erase", 
        help="Erase the entire datasets folder",
        type=str,
        default=DO_ERASE,
    )
    group_parser.add_argument(
        "--do_unzip", 
        help="Unzip all zip files",
        type=str,
        default=DO_UNZIP,
    )
    group_parser.add_argument(
        "--do_delete_zip", 
        help="Erase all intermediate zip files",
        type=str,
        default=DO_DELETE_ZIP,
    )


    group_parser = parser.add_argument_group("Spreadsheet-related options")
    group_parser.add_argument(
        "--spreadsheet_id", 
        help="Google Drive document ID of the spreadsheet to download",
        type=str,
        default=SPREADSHEET_ID,
    )
    group_parser.add_argument(
        "--spreadsheet_filename", 
        help="Google Drive document ID of the spreadsheet to download",
        type=str,
        default=SPREADSHEET_FILENAME,
    )


    args, _ = parser.parse_known_args()

    # 1. Download the datasets_index spreadsheet
    download_file_from_google_drive(args.spreadsheet_id, args.spreadsheet_filename)
    df = pd.read_csv(args.spreadsheet_filename)

    # 2. Scan the local datasets to update the status of each dataset

    # (Optional : execute the actions)
    
    # 3. Download the datasets
    table = Table(
        title=f"De profundiS : Summary of the datasets",
        show_lines=True,
    )

    table.add_column(header="Project", footer="Project", justify="left", style="green", no_wrap=True)
    table.add_column(header="Task", footer="Task", justify="left", style="cyan")
    table.add_column(header="Name", footer="Name", justify="left", style="magenta")
    table.add_column(header="Status", footer="Status", justify="left", style="magenta")
    table.add_column(header="Total size", footer="Total size", justify="left", style="magenta")
    table.add_column(header="Is manual", footer="Is manual", justify="left", style="magenta")
    table.add_column(header="# of URLs", footer="# of URLs", justify="left", style="magenta")

    for yt in self.downloads:
        table.add_row(
            yt.video_id,
            yt.length,
            yt.title.capitalize(),
            yt.composer.capitalize(),

            ', '.join(yt.keywords[:3]),
            yt.views,
            yt.rating,

            ', '.join([x.capitalize() for x in genres[:3]]),
            yt.year,
            yt.tempo,
            yt.time_signature,
            yt.time_signature_confidence,

            yt.description,
            yt.publish_date,
            yt.age_restricted,
        )

    console.print(table)`
