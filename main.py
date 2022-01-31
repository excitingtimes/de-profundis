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
import glob
from time import time
from time import sleep



# ---
# Scientific imports
# ---
import pandas as pd



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
from rich.prompt import Confirm
from rich.tree import Tree
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
    'unknown': '❓',
}



# ---
# Default CLI parameters' values
# ---
INCLUDE = []
EXCLUDE = []
INCLUDE_TASKS = []
EXCLUDE_TASKS = []
INCLUDE_PROJECTS = []
EXCLUDE_PROJECTS = []
DELETE_ZIP = True
UNZIP = True

HOME_DIR = os.path.expanduser('~')
PROJECTS = list(map(os.path.basename, (glob.glob(os.path.join(HOME_DIR, "*")))))
PROJECTS.remove("scratch")
ROOT_DIR = os.path.join(HOME_DIR, "scratch/")

DO_RESET_SCRATCH = False
DO_ERASE_SCRATCH = False
DO_BUILD_SCRATCH = False
DO_ERASE = False
DO_UNZIP = False
DO_DELETE_ZIP = False

CLEAN_CHECKPOINTS = False
CLEAN_LOGS = False
CLEAN_OUTPUTS = False

SPREADSHEET_ID = "10ftkEU-FQsGCrrUW4lSuip0g5joUWD_04EJtUeywrLM"
SPREADSHEET_FILENAME = "datasets_index.csv"

VERBOSE = False
VISUAL_GUI = False



def download_file_from_google_drive(
    id,
    destination,
):
    def get_confirm_token(
        response,
    ):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value

        return None

    def save_response_content(
        response, 
        destination,
    ):
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



def process_df(
    df,
    args,
): 
    df["is_manual"] = df["URL(s)"].apply(lambda x: (x == "manual"))
    df["URL(s)"] = df["URL(s)"].apply(lambda x: (x.split(";") if x != "manual" else []))
    df["Status"] = STATUSES['unknown']
    df["Total size"] = 1



def process_actions(
    df,
    args,
):
    if args.do_reset_scratch:
        args.do_erase_scratch = True
        args.do_build_scratch = True
    if args.do_erase_scratch:
        if Confirm.ask("[logging.keyword]Are you sure you want to erase 'scratch/' ?[/logging.keyword]"):
            console.log("[logging.level.debug]Erasing 'scratch/' ...[/logging.level.debug]")
        else:
            console.log("[logging.level.error]Did not delete 'scratch/' ![/logging.level.error]")

    if args.do_build_scratch:
        console.log("[logging.level.info]Building 'scratch/' ...[/logging.level.info]")
        if not os.listdir(SCRATCH_DIR):
            console.log("Directory 'scratch/' is empty.")
        else:
            console.log("Directory 'scratch/' is empty. Will not overwrite existing folders.")
    elif args.do_erase:
        if Confirm.ask("[logging.keyword]Are you sure you want to erase the datasets folder ?[/logging.keyword]"):
            console.log("[logging.level.debug]Erasing the 'datasets/' folder ...[/logging.level.debug]")
        else:
            console.log("[logging.level.error]Did not delete delete the 'datasets/' folder ![/logging.level.error]")

    if args.do_unzip:
        console.log("[logging.level.info]Unzipping intermediate ZIP files ...[/logging.level.info]")
    if args.do_delete_zip:
        if Confirm.ask("[logging.keyword]Are you sure you want to delete intermediate ZIP files ?[/logging.keyword]"):
            console.log("[logging.level.debug]Deleting intermediate ZIP files ...[/logging.level.debug]")
        else:
            console.log("[logging.level.error]Did not delete intermediate ZIP files ![/logging.level.error]")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="De profundiS : Datasets Sycnhronizer utility",
    )


    group_parser = parser.add_argument_group("Download-related options")
    group_parser.add_argument(
        "--include", 
        help="Specific datasets to exclude (only those given in argument will be downloaded)",
        type=list,
        default=INCLUDE,
    )
    group_parser.add_argument(
        "--exclude", 
        help="Specific datasets to exclude (all other datasets will be downloaded)",
        type=list,
        default=EXCLUDE,
    )
    group_parser.add_argument(
        "--include_tasks", 
        help="Specific tasks to include (only those given in argument will be downloaded)",
        type=list,
        default=INCLUDE_TASKS,
    )
    group_parser.add_argument(
        "--exclude_tasks", 
        help="Specific tasks to exclude (all other tasks' datasets will be downloaded)",
        type=list,
        default=EXCLUDE_TASKS,
    )
    group_parser.add_argument(
        "--include_projects", 
        help="Specific project to include (only those given in argument will be downloaded)",
        type=list,
        default=INCLUDE_PROJECTS,
    )
    group_parser.add_argument(
        "--exclude_projects", 
        help="Specific project to exclude (all other projects' datasets will be downloaded)",
        type=list,
        default=EXCLUDE_PROJECTS,
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


    group_parser = parser.add_argument_group("Actions")
    group_parser.add_argument(
        "--do_reset_scratch", 
        help="Reset the 'scratch/' folder",
        type=str,
        default=DO_RESET_SCRATCH,
    )
    group_parser.add_argument(
        "--do_erase_scratch", 
        help="Erase the entire 'scratch/' folder",
        type=str,
        default=DO_ERASE_SCRATCH,
    )
    group_parser.add_argument(
        "--do_build_scratch", 
        help="Build the architecture of the 'scratch/' folder",
        type=str,
        default=DO_BUILD_SCRATCH,
    )
    group_parser.add_argument(
        "--do_erase", 
        help="Erase the entire 'datasets/' folder",
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


    group_parser = parser.add_argument_group("Cleaning routines")
    group_parser.add_argument(
        "--clean_checkpoints", 
        help="Cleans the 'checkpoints/' subfolder. If no value is provided, cleans checkpoints for all projects. Otherwise, only cleans checkpoints for the selected projects.",
        type=None,
        default=CLEAN_CHECKPOINTS,
    )
    group_parser.add_argument(
        "--clean_logs", 
        help="Cleans the 'logs/' subfolder. If no value is provided, cleans logs for all projects. Otherwise, only cleans logs for the selected projects.",
        type=None,
        default=CLEAN_LOGS,
    )
    group_parser.add_argument(
        "--clean_outputs", 
        help="Cleans the 'outputs/' subfolder. If no value is provided, cleans outputs for all projects. Otherwise, only cleans outputs for the selected projects.",
        type=None,
        default=CLEAN_OUTPUTS,
    )


    group_parser = parser.add_argument_group("Path-related options")
    group_parser.add_argument(
        "--root", 
        help="Root directory (default $SCRATCH) containing the datasets/ folder",
        type=str,
        default=ROOT_DIR,
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


    group_parser = parser.add_argument_group("Visual options")
    group_parser.add_argument(
        "--verbose", 
        help="Set to true to get advanced debug info",
        type=bool,
        default=VERBOSE,
    )
    group_parser.add_argument(
        "--visual_gui", 
        help="Get a visual GUI to dynamically explore the status of the datasets",
        type=bool,
        default=VISUAL,
    )

    args, _ = parser.parse_known_args()

    console.log(args)

    # 1. Download the datasets_index spreadsheet
    print(Panel(Text("1. Downloading the spreadsheet", justify="center")))
    try:
        download_file_from_google_drive(
            args.spreadsheet_id, 
            args.spreadsheet_filename,
        )
        df = pd.read_csv(
            args.spreadsheet_filename, 
            header=0,
        )
        console.log(f"Successfully downloaded the datasets spradsheet from Google Docs : ID={args.spreadsheet_id} -> {args.spreadsheet_filename}")
    except:
        raise Exception("SpreadsheetUnavailable")

    process_df(
        df, 
        args,
    )
    home_dir = os.path.join(HOME_DIR)
    root_dir = args.root
    scratch_dir = os.path.join(root_dir, "scratch/")
    datasets_dir = os.path.join(scratch_dir, "datasets/")
    checkpoints_dir = os.path.join(scratch_dir, "checkpoints/")
    outputs_dir = os.path.join(scratch_dir, "outputs/")
    logs_dir = os.path.join(scratch_dir, "logs/")

    # 2. Scan the local datasets to update the status of each dataset
    # MainView.run(title="[bold] [italic] De profundiS [/italic] [/bold] : Datasets Sycnhronizer utility", log="textual.log")

    console.log(df)
    tree = Tree("Directory")
    hd = tree.add(f"[markdown.strong]home_dir[/markdown.strong] = [markdown.emph]{home_dir}[/markdown.emph]")
    tree.add("[markdown.strong]projects[/markdown.strong] : " + ', '.join(PROJECTS))
    sc = hd.add(f"[markdown.strong]scratch_dir[/markdown.strong] = [markdown.emph]{scratch_dir}[/markdown.emph]")
    hd.add(f"[markdown.strong]root_dir[/markdown.strong] = [markdown.emph]{root_dir}[/markdown.emph]")
    sc.add(f"[markdown.strong]datasets_dir[/markdown.strong] = [markdown.emph]{datasets_dir}[/markdown.emph]")
    sc.add(f"[markdown.strong]checkpoints_dir[/markdown.strong] = [markdown.emph]{checkpoints_dir}[/markdown.emph]")
    sc.add(f"[markdown.strong]outputs_dir[/markdown.strong] = [markdown.emph]{outputs_dir}[/markdown.emph]")
    sc.add(f"[markdown.strong]logs_dir[/markdown.strong] = [markdown.emph]{logs_dir}[/markdown.emph]")
    print(tree)

    # 2. If actions are specified, execute the actions
    print(Panel(Text("2. Executing actions", justify="center")))
    process_actions(
        df, 
        args,
    )
    
    # 3. Select the relevant datasets
    print(Panel(Text("3. Select relevant datasets", justify="center")))
    select_datasets(
        df,
        args,
    )

    # 4. Update the statuses of the datasets in the database
    print(Panel(Text("4. Update the database", justify="center")))
    update_statuses(
        df,
        args,
    )

    # 5. Download datasets
    print(Panel(Text("5. Download datasets", justify="center")))
    download_datasets(
        df,
        args,
    )

    # 6. Extract datasets
    print(Panel(Text("6. Extract datasets", justify="center")))
    extract_datasets(
        df,
        args,
    )

    # 7. Summary of the operations
    print(Panel(Text("7. Summary", justify="center")))
    summary(
        df,
        args,
    )