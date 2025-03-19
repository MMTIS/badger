import re
import logging
from typing import IO, Any

from lxml import etree
from typing_extensions import Pattern

from utils.aux_logging import prepare_logger, log_all, log_print, log_flush
import traceback

from netexio.dbaccess import open_netex_file


def process_file_in_chunks(
    file: IO[Any], patterns: list[str], chunk_size: int = 1024
) -> dict[str, int]:
    """
    Process a file in chunks and apply multiple regex patterns that can span multiple lines.

    :param file: the file
    :param patterns: List of regex patterns to be applied
    :param chunk_size: Size of the chunks to read from the file
    :return: Dictionary with pattern as key and count of matches as value
    """
    compiled_patterns: list[Pattern[str]] = [
        re.compile(pattern, re.MULTILINE | re.DOTALL) for pattern in patterns
    ]
    buffer: str = ""
    match_counts: dict[str, int] = {pattern: 0 for pattern in patterns}
    while True:
        chunk = file.read(chunk_size)
        if not chunk:
            break
        buffer += chunk

        for i, regex in enumerate(compiled_patterns):
            while True:
                match = regex.search(buffer)
                if match:
                    match_counts[patterns[i]] += 1
                    buffer = buffer[match.end() :]
                else:
                    break

        if len(buffer) > chunk_size:
            buffer = buffer[-chunk_size:]

        # Process any remaining buffer after reading the file

    for i, regex in enumerate(compiled_patterns):
        while True:
            match = regex.search(buffer)
            if match:
                match_counts[patterns[i]] += 1
                buffer = buffer[match.end() :]
            else:
                break

    return match_counts

def process_bytes(
    bytes: bytes, patterns: list[str], chunk_size: int = 1024
) -> dict[str, int]:
    """
    Process a file in chunks and apply multiple regex patterns that can span multiple lines.

    :param file: the file
    :param patterns: List of regex patterns to be applied
    :param chunk_size: Size of the chunks to read from the file
    :return: Dictionary with pattern as key and count of matches as value
    """
    compiled_patterns: list[Pattern[str]] = [
        re.compile(pattern, re.MULTILINE | re.DOTALL) for pattern in patterns
    ]
    buffer: str = ""
    match_counts: dict[str, int] = {pattern: 0 for pattern in patterns}
    while True:
        chunk = file.read(chunk_size)
        if not chunk:
            break
        buffer += chunk

        for i, regex in enumerate(compiled_patterns):
            while True:
                match = regex.search(buffer)
                if match:
                    match_counts[patterns[i]] += 1
                    buffer = buffer[match.end() :]
                else:
                    break

        if len(buffer) > chunk_size:
            buffer = buffer[-chunk_size:]

        # Process any remaining buffer after reading the file

    for i, regex in enumerate(compiled_patterns):
        while True:
            match = regex.search(buffer)
            if match:
                match_counts[patterns[i]] += 1
                buffer = buffer[match.end() :]
            else:
                break

    return match_counts

def do_contains(file_path: IO[Any], patterns: list[str]) -> bool:
    match_counts = process_file_in_chunks(file_path, patterns, chunk_size=1024)
    for match, count in match_counts.items():
        if count > 0:
            log_print(f'Assertion passed: File {file_path}: contains regex "{match}"')
        else:
            log_all(
                logging.ERROR,
                f'Assertion failed: File {file_path} does not contain regex "{match}"',
            )
        return False
    return True


def main(assertions_file: str, input_file: str) -> None:
    # Define the namespace URI
    namespace_uri = "http://www.netex.org.uk/netex"
    # Create the namespace map with the URI as the value
    namespaces = {"netex": namespace_uri}
    file_generator = open_netex_file(input_file)
    with open(assertions_file, "r", encoding="utf-8") as file:
        assertions = file.readlines()
    # sort out all relevant regex
    patterns: list[str] = []
    for assertion in assertions:
        assertion = assertion.strip()
        if assertion.startswith("contains"):
            regex = assertion.split(" ", 1)[1]
            patterns.append(regex)
    #
    for sub_file in file_generator:
        log_print(f"working on {sub_file}")
        input = sub_file.read()
        tree = etree.fromstring(input)
        if not do_contains(input, patterns):
            return
        failed = 0
        for assertion in assertions:
            assertion = assertion.strip()
            if assertion.startswith("#"):
                comment = assertion.split(" ", 1)[1]
                log_print(f"comment: {comment}")
            elif assertion.startswith("contains"):
                log_print("skip contains")
            elif assertion.startswith("xpathcountequal"):
                parts = assertion.split(" ")
                xpath_expression = parts[1]
                expected_count = int(parts[2])
                results = tree.xpath(xpath_expression, namespaces=namespaces)
                if len(results) == expected_count:
                    log_print(
                        f'Assertion passed: {sub_file}: XPath "{xpath_expression}" has {expected_count} results'
                    )
                else:
                    log_all(
                        logging.ERROR,
                        f'Assertion failed: {sub_file}: XPath "{xpath_expression}" for {sub_file} does not have {expected_count} results, was {len(results)}',
                    )
                    failed = 1
            elif assertion.startswith("xpathcountgreater"):
                parts = assertion.split(" ")
                xpath_expression = parts[1]
                expected_count = int(parts[2])
                results = tree.xpath(xpath_expression, namespaces=namespaces)
                if len(results) > expected_count:
                    log_print(
                        f'Assertion passeded: {sub_file}: XPath "{xpath_expression}" has more than {expected_count} results, was {len(results)}'
                    )
                else:
                    log_all(
                        logging.ERROR,
                        f'Assertion Failed: {sub_file}: XPath "{xpath_expression}" does not have more than {expected_count} results, was {len(results)}',
                    )
                    failed = 1
            elif len(assertion.strip()) > 0:
                log_all(logging.ERROR, f"Invalid assertion: {assertion}")
                failed = 1
        if failed > 0:
            exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Lets a set of assertions run on a file (xml)"
    )
    parser.add_argument("assertions_file", type=str, help="File with the assertions")
    parser.add_argument("input_file", type=str, help="the input file (xml)")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.assertions_file, args.input_file)
        log_flush()
    except Exception as e:
        log_all(logging.ERROR, traceback.format_exc())
        log_flush()
        raise e
