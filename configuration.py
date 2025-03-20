# CONFIGURATION:
import logging
from typing import Any
import os
# Logging
log_dict: dict[str, list[Any]] = (
    {}
)  # relevant values key - type of problem, then  [count, message]
NOSOFTLOGGING = False  # if set to True the log_print function will output nothing
LOGEXAMPLE = (
    False  # if set to True then tool_check_db will print a random row for each table
)
general_log_level = logging.INFO

main_log_file = "aux.log"  # in the processing_data folder

# FrameDefaults
defaults: dict[str, str | int | bool] = {}
defaults["authority"] = "http://openov.nl/"
defaults["timezone"] = "Europe/Amsterdam"
defaults["particpant_ref"] = "NDOV"
defaults["xml_description"] = "Huge XML Serializer test"
defaults["feed_publisher_name"] = "Publication Delivery"
defaults["feed_publisher_url"] = "http://publicationdelivery.eu"
defaults["os"] = "windows"
defaults["authority_reference"] = True
defaults["codespace"] = "OPENOV"
defaults["version"] = 1

input_dir = "d:/input"  # the standard directory for files to load (depreciated)
processing_data = "d:/aux_testing_processing"   #the root folder for processing
script_path = os.path.dirname(os.path.abspath(__file__))
list_scripts = os.path.join(script_path,"tools/tool_scripts/list_scripts.txt")  # place where the list processing scripts are stored, based from program folder
gtfs_validator = os.path.join(script_path,"tools/gtfs-validator-cli.jar")  # location of the gtfs validator.jar, based from program folder.

# ftp connection information for the ftp upload tool
ftpconns = {
    "sbb_ftp": {
        "server": "ftp.example.com",
        "directory": "/upload/directory",
        "username": "your_username",
        "password": "your_password",
        "port": 21,
    }
}
# a local configuration overwrites the general one. The local_configuration must not be added to github
try:
    from local_configuration import *  # noqa: F403
except ImportError:
    pass
