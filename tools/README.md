# Tools for badger
by Matthias Günter, Stefab de Konink

In this directory there is a set of tools that can be used with the badger conversions:
* a script runner
* another script runner
* a gtfs validator
* a gtfs visualisation on maps
* an xml validator (used to validate the NeTEx with the relevant XSD)

We also have two types of "fixing" tools for the script runner:
* some work on the NeTEx files
* some work on the mdbx database
* one fixes some problematic characters in GTFS stops.txt

The tools are evolving.

The tools are fixing different problems that occur.
Future tools will contain:
* a relation explorer in NeTEx
* a SIRI ET/PT export of a given operating day
* some assertions for NeTex
* some statistics for NeTEx



## The XML (NeTEx) validator
in the `tools` folder there is the a tool for validating xml files.
```
uv run python -m tools.xml_validator path_to_xml_file.xml.zip --log_file <path> --xsdschema <path> 
```
`path_to_xml_file.xml.zip` can be an xml, a zip or a gz file. If no xsdschema is provided the one from the project is used.


## The GTFS validator
In the `tools` folder there is the a tool for validating GTFS files.
It uses the Mobility Data validator: https://github.com/MobilityData/gtfs-validator/#readme
You need to install this locally, with an accepted Java runtime environment 
(the needed java version is defined by the version of the validator you use).
The current project expects it in the tools-folder.

You can change the name and place in the configuration: 

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
Note:
* We provide some script files in the `tools/tools_script` folder-
* `no_of_step` is the sequence number in the block to start. Be aware that it is the n-th script in the block and not the number/id in `step` in the script.
* `name_of_block` the identier of the block. `all` means all blocks (are processed sequentially.)
* We will in short rename the .txt files to json.

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
* `process_url_list`: see separate section on this option.

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
Predefined list processings (in tools/tool_scripts/list_scripts.txt):
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

## Pre-Post-processors
The preprocessors are currently not uniform and probably not stable. Most those processors function only in a limited way. They don't guarantee consistent results,
when the input file is not suitable for application.
* tools.netex_uicoperatingperiod_correction <input-file> <output-file>: makes sure that the nameOfClassRef of an OperatingPeriodRef is UiCOperatingPeriodRef, when it should be.
* tools.gtfs_sanitize_stops <input-file> <output-file>: In some cases the UTF-8 is not adhered to. This removes the illegal characters from stops.txt. The source data should be corrected.
* fix.add_stop.py <mdbx-database>: French data often does not correctly use PointinServiceJourneyPatternRef in PassingTime. it is not there and the assumption is, that is processed somehow. This functions adds the information.
* tools.netex_preproc_cleanup <input-file> <output-file> [list of actions]: Applies some actions to an input file. The list of actions follow below. 

At a given point we will (a) try to automate detection of necessary processing and (b) include it in a direct data processing pipeline, with some sort of plugins.

Currently, the actions can occur in different steps of the pipeline.

Note: <input-file> and <output-file> must have the same ending zip, xml or xml.gz.  Otherwise the transforms fail.

### Actions of processing steps.
Attention: An empty action list [] tries to apply every action. This is *not* advised. Carefully atune each
processing script to the input data. In some cases the data source should improve the data. The current situation
are fixes.

* VERSIONREF: The attribute versionRef is moved to version. With the exception of "TypeOfFrameRef".
* REMOVEUNNECESSARYIDTAGS: removes id and version from Centroid and Location and reponsibilitySets
* FIXLINESTRINGID: Gml needs id to start not with a number, so we add a prefix.
* ADDIDVERSION: It is sometimes easier to add artifical id and version attributes to a list of elements: "AlternativeName","AlternativeText", "OperatorRef","DayTypeRef","LineRef", "ScheduledStopPointRef", "ServiceJourneyPatternRef", "PassingTime","StopPointInJourneyPatternRef","TimetabledPassingTime"),
* FIXORDER0: The order attribute can't be 0. We set it to 1.
* INCLUDEORDERINID: Previous to NeTEx 2.0 the order attribute was part of the key. So sometimes elements are only unique when order is used. So we append the order to the id. As this does not fix refs. We do it only for elements that are not referenced usually: " in actions_set or not actions_set: "NoticeAssignment", "PassengerStopAssignment","AlternativeName"
* SIMPLIFYVERSION: As validators can deal with the fact that the version="all" can match other versions between id/ref. We make it explicit. any is replaces with the most used version. This only works, if version handling is not used in full.
* FIXEMAILNONE: We had data sets, where the eMail was "None". This is not valid in GTFS. so we change it.
* ADDHTTPSURL: The GTFS validator wants https: in the url of agency.txt. So it is added.
* REMOVESOMEREFS: In some cases Ref elements are used to store codes (e.g. BrandingRef). This is not working with full validation. So we remove them: "SupplyContactRef","TopographicPlaceRef", "ParentSiteRef","TypeOfPlaceRef","BrandingRef"
* REMOVESOMEATTRS: removes datasourceref attribute. To be done, when it is not defined.
* REMOVESNCFPROBLEMS: Remove elements DestinationDisplayRef and OperatorRef when they have attribute ref == "". Remove elements TypeOfLineRef and routes unconditionally. Remove attributes responsibilitySetRef when value == "".
* REMOVELUXPROBLEMS: nameOfClassRef in FromJourney and ToJourney is wrong and is replaced.
* NONE: No action

## The ftp_uploader (untested)
In the `tools` folder there is the a tool for ftp upload.
This is in line the pipeline approach for the script runner.
```
uv run python -m tools.ftp_uload path_to_file config
```
`config` must be defined in the configuration.
