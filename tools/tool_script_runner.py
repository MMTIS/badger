import logging
import os
import time
import json
import shutil
from typing import Any, Optional

from utils.aux_logging import (
    prepare_logger,
    log_all,
    log_flush,
    log_write_counts,
    log_print,
)
from configuration import defaults, processing_data, input_dir, list_scripts
import urllib.request
from datetime import datetime
import re
import hashlib


def custom_hash(value: str) -> str:
    sha256_hash = hashlib.sha256()
    sha256_hash.update(value.encode("utf-8"))
    return sha256_hash.hexdigest()[-8:]


def reversedate() -> str:
    # Get the current date
    current_date = datetime.now()
    # Format the date as YYYYMMDD
    formatted_date = current_date.strftime("%Y%m%d")
    return formatted_date


def parse_command_line_arguments(input_string: str) -> list[str]:
    arguments = re.findall(r"\[.*?\]|\S+", input_string)
    result = []
    for argument in arguments:
        if argument.startswith("[") and argument.endswith("]"):
            # Argument is a list enclosed in square brackets
            list_string = argument[1:-1].strip()
            if list_string:
                # Split the list string and add individual elements to the result
                sublist = list_string.split()
                result.append(sublist)
        else:
            # Argument is a single value
            result.append(argument)
    return result


def create_list_from_string(input_string: str) -> list[str]:
    # Remove the square brackets from the string
    cleaned_string = input_string.strip("[]")
    # Split the cleaned string into a list using space as the delimiter
    result_list = cleaned_string.split(" ")
    return result_list


def check_string(input_string: str) -> bool:
    if "[" in input_string and "]" in input_string:
        return True
    else:
        return False


def load_and_run(file_name: str, args_string: str) -> Any:

    module_name = file_name.rstrip(".py")
    components = module_name.split(".")
    #mod=None
    #for comp in components:
    #    if mod==None:
    #        mod =__import__(comp)
    #    else:
    #        mod= getattr(mod,comp)
    # main_function= getattr(mod,"main")
    mod = __import__(module_name,fromlist=[None])
    main_function=getattr(mod,"main")

    if not callable(main_function):
        raise TypeError(f"{module_name} is not callable!")

    args = parse_command_line_arguments(args_string)
    # args = args_string.split()  # Split the string into a list of arguments
    args1: list[Any] = []
    for arg in args:
        result: Any
        result=arg
        if check_string(arg):
            print(".")
            #arg= create_list_from_string(arg)
            result = arg
        elif arg == "True":
            result = True
        elif arg == "False":
            result = False
        args1.append(result)
    result = main_function(*args1)
    return result


def replace_in_string(input: str, search: str, replace: str) -> str:
    if search in input and (replace == "" or replace == "NOT SET YET"):
        raise ValueError(
            f"Replace string cannot be empty when search string ({search})exists in input."
        )
    return input.replace(search, replace)


# command to clean a directory from temp files (mostly duckdb)
def clean_tmp(f: str) -> None:
    # Iterate over the items in the folder
    if not os.path.isdir(f):
        log_all(logging.WARNING, f"No valid path to clean:{f}")
        return

    for item in os.listdir(f):
        item_path = os.path.join(f, item)

        if os.path.isfile(item_path):
            # Remove file if it matches the extensions
            if (
                item.endswith(".duckdb")
                or item.endswith(".tmp")
                or item.endswith("lmdb")
                or item.endswith("mdb")
            ):  # logs are NOT cleaned (as at least one is already locked)
                try:
                    os.remove(item_path)
                except Exception as e:
                    log_print(f"Error while removing file: {e}")
        elif os.path.isdir(item_path):
            # Recursively clean subdirectory
            clean_tmp(item_path)


# removes a given processed folder
def clean(directory: str) -> None:
    # Clean the specified folder by deleting all files and subfolders
    if not os.path.isdir(directory):
        log_all(logging.WARNING, f"No valid path to clean: {directory}")
        return

    # Iterate over the items in the directory
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isfile(item_path):
            # Remove file
            try:
                os.remove(item_path)
            except OSError:
                log_print(f"Could not remove {item_path}")

        elif os.path.isdir(item_path):
            # Remove subdirectory and its contents
            try:
                shutil.rmtree(item_path)
            except OSError:
                log_print(f"Could not remove {item_path}")


def parse_key_value_pairs(string: str) -> dict[str, str]:
    pairs = {}
    for pair in string.split(";"):
        key_value = pair.split("=")
        if len(key_value) == 2:
            key = key_value[0].strip()
            value = (
                key_value[1].strip().strip('"').strip("'")
            )  # Remove surrounding quotes from value
            pairs[key] = value
    return pairs


def set_defaults(keyvaluestr: str) -> None:
    result = parse_key_value_pairs(keyvaluestr)
    # replace what is not yet in defaults
    defaults.update(result)


def download(folder: str, url: str, forced: bool = False) -> str:
    try:
        # Create the folder if it doesn't exist
        if not os.path.exists(folder):
            os.makedirs(folder)

        # Get the filename from the URL
        filename = os.path.basename(url)
        # work around for swiss data, where it is "permalink"
        if filename == "permalink":
            filename = "swiss.zip"
        if (
            "?" in filename
        ):  # for data from mobigo, that is fetched by an aspx script with parameters
            filename = "source.zip"
        if "Resource" in filename:  # for italian data
            filename = "source.xml.gz"
        if filename == "":
            filename = "source.zip"
        if not forced:
            # Download only when not exists
            path = os.path.join(folder, filename)
            if os.path.exists(path):
                log_all(logging.INFO, f"File: {path} exists already.Will use that one")

        # Download the file
        log_all(logging.INFO, f"Download from: {url}")
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = [("User-Agent", "MyApp/1.0")]
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(url, os.path.join(folder, filename))
            log_all(logging.INFO, "File downloaded successfully.")

        except urllib.error.HTTPError as e:
            log_all(logging.ERROR, f"HTTP Error: {e.code} - {e.reason}")
        except urllib.error.URLError as e:
            log_all(logging.ERROR, f"URL Error: {e.reason}")

        # Return the downloaded file path
        return os.path.join(folder, filename)

    except urllib.error.HTTPError:
        return "FILE NOT FOUND"


def remove_file(path: str) -> str:
    if os.path.isfile(path):
        try:
            os.remove(path)
            return "File removed successfully."
        except OSError as e:
            raise OSError(f"Failed to remove file: {str(e)}")
    else:
        raise FileNotFoundError(f"File not found: {path}")


def main(
    script_file: str,
    log_file: str,
    log_level: int,
    todo_block: str,
    begin_step: int = 1,
    end_step: int = 99999,
    this_step: int = -1,
    url: Optional[str] = None,
    parent_block: str = "",
) -> None:
    # blockexisted
    blockexisted = False
    # Read the scripts from a file
    with open(script_file) as f:
        data = json.load(f)

    # go through each block
    for block in data:
        if url:
            processdir = (
                processing_data
                + "/"
                + parent_block
                + "-"
                + todo_block
                + "-"
                + str(custom_hash(url))
            )
        else:
            processdir = processing_data + "/" + block["block"]
        blockstop = False
        if not todo_block == block["block"]:
            if not todo_block == "all":
                continue
        # make sure folder for block exists
        os.makedirs(processdir, exist_ok=True)
        blockexisted = True
        scripts = block["scripts"]
        prepare_logger(log_level, block["block"] + "/" + log_file)
        # log_once(logging.INFO, "Start", f'Processing block: {block["block"]}')
        step = 0
        script_input_file_path = "NOT SET YET"
        for script in scripts:
            step = step + 1
            # skip some steps if this is mandated
            if step != this_step:
                if step < begin_step:
                    continue
                if blockstop:
                    break
                if step > end_step:
                    # only process until here
                    break
            else:
                blockstop = True  # we only process this one step

            if "download_urls" not in block.keys() and (
                step < begin_step
            ):  # if it is a list we always begin with 1 the begin_step is then used within the list
                continue
            if blockstop:
                break
            start_time = time.time()

            script_name = script["script"]
            script_args = script["args"]
            if url:
                script_download_url = url
            else:
                script_download_url = block.get("download_url")
            # replace the placeholder for processdir with the correct values and also the other place holders
            script_args = replace_in_string(script_args, "%%dir%%", processdir)
            script_args = replace_in_string(script_args, "%%inputdir%%", input_dir)
            script_args = replace_in_string(
                script_args, "%%inputfilepath%%", script_input_file_path
            )
            script_args = replace_in_string(script_args, "%%block%%", block["block"])
            script_args = replace_in_string(
                script_args, "%%log%%", block["block"] + "/" + log_file
            )
            script_args = replace_in_string(script_args, "%%date%%", reversedate())

            # if the processing dir doesn't exist, then we create it
            os.makedirs(processdir, exist_ok=True)

            # Write the script name to the log file with a starting delimiter
            log_all(
                logging.INFO,
                f"{block['block']} - step: {step}: {script_name} {script_args}",
            )

            if script_name.startswith("#"):
                # is a comment and we do nothing
                continue

            if script_name == "set_defaults":
                # Sets default values (when not done in configuration.py or local_configuration.py)
                set_defaults(script_args)
                log_all(
                    logging.INFO,
                    f"Command 'set_defaults' executed for: {script_args}\n",
                )
                continue

            if script_name == "clean_tmp":
                # Execute the clean_tmp command
                folder = script_args
                clean_tmp(folder)
                log_all(
                    logging.INFO, f"Command 'clean_tmp' executed for folder: {folder}\n"
                )
                continue
            if script_name == "process_url_list":
                for url in block.get("download_urls"):
                    newblock = script_args
                    main(
                        list_scripts,
                        log_file,
                        log_level,
                        newblock,
                        begin_step,
                        url=url,
                        parent_block=block["block"],
                    )
                # only one process_url_list can be in a block
                return
            if script_name == "clean":
                # Execute the clean command
                folder = script_args
                clean(folder)
                log_all(
                    logging.INFO, f"Command 'clean' executed for folder: {folder}\n"
                )
                continue
            if script_name == "download_input_file":
                # Execute the download command. The file under the download_url is copied to a folder
                folder = script_args
                script_input_file_path = download(folder, script_download_url)
                if script_input_file_path == "FILE NOT FOUND":
                    log_all(logging.ERROR, "No file downloaded")
                    exit(1)
                log_all(
                    logging.INFO,
                    f"Command 'download_input_file' executed for url: {script_download_url}\n",
                )
                continue
            if script_name == "process_url_list":
                for url in block.get("download_urls"):
                    newblock = script_args
                    main(
                        list_scripts,
                        log_file,
                        log_level,
                        newblock,
                        begin_step,
                        url=url,
                        parent_block=block["block"],
                    )
                # only one process_url_list can be in a block
                return
            if script_name == "remove_file":
                # Execute the download command. The file under the download_url is copied to a folder
                remove_file(script_input_file_path)
                log_all(
                    logging.INFO,
                    f"Command 'remove_file' executed for file: {script_input_file_path}\n",
                )
                continue
            result = load_and_run(script_name, script_args)
            end_time = time.time()
            execution_time = int(10 * (end_time - start_time)) / 10

            # Write the execution time to the log file
            log_all(
                logging.INFO,
                f"Execution time: {execution_time} seconds for {block['block']} - step: {step}: {script_name} {script_args}\n",
            )
            log_write_counts(logging.WARNING)
            log_flush()
            if result is None or result == 0:
                log_all(logging.DEBUG, f"Script {script_name} successfully terminated.")
                log_flush()
            elif result == 1:
                log_all(
                    logging.ERROR,
                    f"Script {script_name} returned an error. Terminating the block of scripts: {block['block']}",
                )
                log_flush()
                blockstop = True
                break
            else:
                log_all(
                    logging.ERROR,
                    f"Script {script_name} returned an unexpected error code: {result.returncode}.",
                )
                log_flush()
                blockstop = True
                break
    if not blockexisted:
        log_all(logging.ERROR, f'Block "{todo_block}" not in script file.')
        log_flush()


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Executes scripts")
    parser.add_argument("script_file", type=str, help="the script file")
    parser.add_argument("log_file", type=str, help="name of the log file")
    parser.add_argument("blockname", type=str, help="Block name to do")
    parser.add_argument(
        "--begin_step", type=int, default=1, help="The begin step (default: 1)"
    )
    parser.add_argument(
        "--end_step",
        type=int,
        default=999999,
        help="last step to execute. default not set.",
    )
    parser.add_argument(
        "--this_step", type=int, default=-1, help="not set. Only this step is done"
    )
    parser.add_argument(
        "--log_level",
        type=int,
        default=logging.INFO,
        help="The log level (use logging constants)",
    )
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(
            args.script_file,
            args.log_file,
            args.log_level,
            args.blockname,
            begin_step=args.begin_step,
            end_step=args.end_step,
            this_step=args.this_step,
        )
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
