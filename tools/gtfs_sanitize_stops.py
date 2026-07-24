import shutil
import tempfile
import zipfile
import logging
import os
from utils.aux_logging import prepare_logger, log_all
import traceback

def fix_stops_in_gtfs_zip(gtfs_zip_path: str, replacement: str = '?') -> None:
    """
    Open a GTFS zip file, read stops.txt, replace any invalid UTF-8 bytes
    with the provided replacement character and write stops.txt back into
    the zip. Other files are preserved unchanged.

    Args:
        gtfs_zip_path: Path to the GTFS zip file.
        replacement: Single-character string used to replace invalid bytes
                     when decoding stops.txt. Default is '?'.

    Raises:
        FileNotFoundError: if gtfs_zip_path does not exist.
        ValueError: if replacement is not a single-character string.
    """
    if not os.path.isfile(gtfs_zip_path):
        raise FileNotFoundError(f"{gtfs_zip_path} not found")
    if not isinstance(replacement, str) or len(replacement) != 1:
        raise ValueError("replacement must be a single character string")

    # Create temporary file for the new zip
    dir_name = os.path.dirname(os.path.abspath(gtfs_zip_path))
    fd, tmp_path = tempfile.mkstemp(suffix='.zip', dir=dir_name)
    os.close(fd)

    try:
        with zipfile.ZipFile(gtfs_zip_path, 'r') as zin, \
             zipfile.ZipFile(tmp_path, 'w', compression=zipfile.ZIP_DEFLATED) as zout:

            # Track whether stops.txt was found
            found_stops = False

            for item in zin.infolist():
                name = item.filename
                # read raw bytes for each file
                data = zin.read(name)

                if name == 'stops.txt':
                    found_stops = True
                    # Try to decode as utf-8; on error, replace invalid sequences with replacement char
                    # Python's 'surrogateescape' or 'replace' codecs don't allow customizing the replacement char,
                    # so we implement a safe decode by using 'utf-8' with 'replace' and then mapping the standard
                    # replacement U+FFFD to the requested replacement character.
                    text = data.decode('utf-8', errors='replace')
                    if replacement != '\ufffd':
                        # Replace the Unicode replacement character with the desired replacement.
                        # This is safe because we only introduced U+FFFD at positions of invalid bytes.
                        text = text.replace('\ufffd', replacement)
                    # Write back as utf-8 bytes
                    new_data = text.encode('utf-8')
                    zout.writestr(item, new_data)
                else:
                    # copy other files preserving their metadata
                    zout.writestr(item, data)

            if not found_stops:
                # No stops.txt found; just replace original zip with copied one (essentially identical)
                pass

        # Replace original zip with the new one atomically
        shutil.move(tmp_path, gtfs_zip_path)

    except Exception:
        # Cleanup temp file on error
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        raise



def main(gtfs_zip_path: str) -> None:
    fix_stops_in_gtfs_zip(gtfs_zip_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Removes illegal characters from a GTFS stops.txt file. Needed for Italian data")
    parser.add_argument("gtfs_file", type=str, help="The input file (gtfs.zip)")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.gtfs_file, args.res_folder)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
