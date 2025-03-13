# Tools for badger
by Matthias Günter

In this directory there is a set of tools that can be used with the badger conversions:
* a script runner
* a gtfs validator
* a gtfs visualisation
* an xml validator (used to validate the NeTEx with the relevant XSD)

Future tools will contain:
* a relation explorer in NeTEx
* a SIRI ET/PT export of a given operating day
* some assertions for NeTex
* some statistics for NeTEx


We might also add tools, that correct common problems in certain data sets.

## The XML (NeTEx) validator
in the `tools` folder there is the a tool for validating xml files.
```
uv run python -m tools.xml.validator path_to_xml_file.xml.zip  path_to_main_xsd_file
```
`path_to_main_xsd_file` is in `../schema/netex/xsd/netex_publication.xsd` if the project is installed regularly.
`path_to_xml_file.xml.zip` can be an xml, a zip or a gz file.

## The GTFS validator
In the `tools` folder there is the a tool for validating GTFS files.
It uses the Mobility Data validator: https://github.com/MobilityData/gtfs-validator/#readme
You need to install this locally, with an accepted Java runtime environment.
The current project expects it in the tools-folder. You can change the name and place in the configuration: 

```
gtfs_validator = "./gtfs-validator-7.0.0-cli.jar"  # location of the gtfs validator.jar.
```
You can run it as
```
uv run python -m tools.gtfs_validator path_to_gtfs_file.zip  path_to_report_file
```
The script calls the jar file to do it works.
The result contains two json-files and an html report.
If there is no error found, then the excecution is considered successful.

## The GTFS Visualisation
In the `tools` folder there is the a tool for GTFS visualisation.
The idea is to see the geographical extension of transport networks.
For this reasons "some" stops and "some" routes are shown on a map.
```
uv run python -m tools.gtfs_map_visualisation path_to_gtfs_file.zip  path_to_folium_map.html one_of_n_routes
```
`one_of_n_routes` is a number.
If set to 1 every route is shown, if set to 10, then only one in 10 routes and one in ten stops is shown.
This is nonsensical for processing, but it shows the geographical extension.
Set to 1 one has a reasonable view on the "lines" within the GTFS file.

On startup in the report only the stops are shown.
To see the trips check the box in the top right corner.

## The script runner
In the `tools` folder there is the a tool for running scripts to process everything.
```
uv run python -m tools.script_runner path_to_script_file.txt  name_of_logfile.log name_of_block (--begin_step=no_of_step) (--this_step=no_of_step) (--end_step=no_of_step)
```
There are predefined script files already in the `tools/tools_script` folder
`no_of_step` is the sequence number of the step (!are counted in the script and not using the "step-no)
`name_of_block` the identier of the block. `all` means all blocks (are processed sequentially.)

### Basic idea
The script runner allows to run scripts.
In the script file there are blocks that can be executed.

### The structure of the script file

The following example shows the example of a script file:
```
  [ {
    "block": "nl4",
    "download_url":"https://github.com/user-attachments/files/18202171/NeTEx_WSF_WSF_20241203_20241203.xml.gz",
    "description": "a description",
    "scripts": [
        {"step":1,"script": "clean_tmp", "args": "%%dir%%"},
        {"step":2,"script": "download_input_file", "args": "%%dir%%"},
        {"step":3,"script": "conv.netex_to_db", "args": "[%%inputfilepath%%] %%dir%%/03.lmdb"},
        {"step":4,"script": "conv.epip_db_to_db", "args": "%%dir%%/03.lmdb %%dir%%/04.lmdb"},
        {"step":5,"script": "conv.epip_db_to_xml", "args": "%%dir%%/04.lmdb %%dir%%/05-epip.xml.gz"},
        {"step":6,"script": "conv.netex_to_db", "args": "[%%dir%%/05-epip.xml.gz] %%dir%%/06.lmdb True"},
        {"step":7,"script": "conv.gtfs_db_to_db", "args": "%%dir%%/06.lmdb %%dir%%/07.lmdb"},
        {"step":8,"script": "conv.gtfs_db_to_gtfs", "args": "%%dir%%/07.lmdb %%dir%%/08-%%block%%-gtfs.zip"},
        {"step":9,"script": "tools.gtfs_validator", "args": "%%dir%%/08-%%block%%-gtfs.zip %%dir%%"},
        {"step":10,"script": "#tools.gtfs_map_visualisation", "args": "%%dir%%/08-%%block%%-gtfs.zip %%dir%%/10-%%block%%-map.html 1"}
         ]
  }]
 ```
Starting a script with `#` means that the the line will be used as a comment (e.g. here step 10).
When starting the processing of a block some variables are replaced:
* `%%dir%%`: the working directory + `/` +blockid
* `%%inputfilepath%%`: When a download is done with the download_url, then `%%inputfilepath%% will contain the newly downloaded file.
* `%%block%%`: The identifier of the block
* `%%inputdir%%`: if stuff should be loaded from a given directory, then `%%inputdir%%` injects it into an argument list. The information is taken from `input_dir` in the configuaration file.

All conversions and tools can be used as `script` with the arguments used as defined.

### Internal commands of the script runner
Some commands are directly implemented in the script runner
* `clean_tmp`: With the given directory all temporary processing files are deleted (usually duckdb and lmdb)
* `download_input_file`: The file specified by `download_url` in the block is stored in the directory indicated.
* `set_defaults`: Superseeds defaults set in the configuraiton file. See the configuration file for allowed elements.
* `process_url_list`: see separate section

Example of `set_defaults`:
```
        {"step":2,"script": "set_defaults", "args": "authority='SKI+'; time_zone='Europe/Berlin'; feed_publisher_name='SBB SKI+'; feed_publisher_url='https://opentransportdata.swiss'"},
 ```
### The special case of the list runner
It is possible to process a list of url (in `download_urls`) with the same block.
Those blocks are defined in `./tools/tool_scripts/list_scripts.txt` (can be changed in the configuration).

Example of the process_url_list commands:
 ```
  [{
        "block":"nl3",
        "description":"nl 3 test with list",
        "download_urls":[
            "https://github.com/user-attachments/files/18202170/NeTEx_WSF_WSF_20241112_20241112.xml.gz"
            ],
        "scripts": [
            {"step":"1", "script": "process_url_list", "args": "netex2epip"}
            ]
       }]
 ```
Normally defined processings:
* `netex2epip`: Form an EPIP from a NeTEx.
* `netex2epipgtfs`: Form EPIP and GTFS from a NeTEx.
* `gtfs2epip`: Forming a GTFS from an EPIP.
* `netex2gtfs`: will be added soon.

Note:
* For each downloaded file a new folder is defined. The block folder will be empty. The folders end with a hash. (we might change this that those are subfolders)
* If using process_url_list, then this is the only step to be used.

### Note
* if using downloads from URL and not starting with that step, then `%%inputfilepath%%` does not have a valid value and will result in an error.
* `this_step` and `end_step` are not tested yet.

## The ftp_uploader (untested)
In the `tools` folder there is the a tool for ftp upload.
This is in line the pipeline approach for the script runner.
```
uv run python -m tools.ftp_uload path_to_file config
```
`config` must be defined in the configuration.
