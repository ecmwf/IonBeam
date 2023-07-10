# IOT Ingester

## Installation

Install from source
```sh
$ git clone ...
$ cd iot_ingester
```

Create a conda environemnt, vevnv or similar
```sh
$ conda env create -f conda_env.yml
$ conda activate iot_ingester
```

Install the package for development
```sh
$ pip install --editable ".[dev]"
```

Recommeneded: Install pre-commit hooks that run ruff, black, isort, flake8, etc on the code before you commit
```sh
pre-commit install
```

## Credentials
Make sure there is a `secrets.yaml` file containing the access credentials for the various , use `example_secrets.yaml` as a template.

## Testing
To run the default battery of smoke tests, just run pytest:
```sh
$ pytest
```

To run against live APIs use:
```sh
$ pytest -m network
```

## Usage
Currently the main way to interact with the pipeline is through the command line interface.
```bash
% ./main.py --help
usage: ECMWF IOT Observation Processor [-h] [--validate-config] [-v] [-j [NUMBER]]
                                       [--finish-after [NUMBER]]
                                       config_file

Put IOT data into the FDB

positional arguments:
  config_file           Path to a yaml config file.

options:
  -h, --help            show this help message and exit
  --validate-config     Just parse the config and do nothing else.
  -v, --verbose         Set the logging level, default is warnings only, -v and -vv
                        give increasing verbosity
  -j [NUMBER], --parallel [NUMBER]
                        Engage parallel mode. Optionally specify how many parallel
                        workers to use, defaults to the number of CPUs + the number of
                        srcs in the pipeline
  --finish-after [NUMBER]
                        If present, limit the number of processed messages to 1 or the
                        given integer
```
The `-vv` and `--finish-after` options are useful for debugging runs.

See [this notebook](examples/notebooks/run_the_pipeline_manually.ipynb) for a walkthrough of assembling the pipeline from various components and then running it. This is useful for debugging the output at various stages in the pipeline.

## The config
There's a lot happening in `examples/config.yaml`. The structure of the file is defined by a set of nested python dataclasses starting with `Config` in `obsproc.core.config_parser`.

The three big pieces are the parsers which define how to rename and clean up the input data, `canonical_variables.yaml` which defines a unique internal name, dtype and unit for each variable and `MARS_keys.yaml` which defines how to spit the data out to ODB format.

## Architecture

### Source:
    yields raw tabular data
    metadata_keys: [`source`]

### Preprocessor:
    renames columns to canonical names
    formats dates to aware UTC dates
    converts units
    splits data into single variables + metadata columns
    metadata_keys: [`source`, `observed_variable`]

### Aggregators:
    Aggregates data by source, variable and time_slice
    In future could merge different sources if appropriate.
    metadata_keys: [`source`, `observed_variable`, `time_slice`]

### Quality Controlers [to implement]:
    - Filters on [`source`, `observed_variable`, `time_slice`], may take more that one unique tuple?
    - does some quality control, ML, whatever
    - yields output data

### Encoders:
    - matches on [`source`, `observed_variable`, `time_slice`]
    - encoders to specified output format, currently CSV, ODB




## Changes
- added CIMA and Meteotracker sources
    - Meteotracker caches raw data to disk, CIMA does not currently

- added a secrets.yaml file for credentials
    - header: useragent: ecmwf scraper from: thomas.hodson@ecmwf.int fields

- changed config parser to load the full config immediately from nested dataclasses
    - gives better and immediate error messages
    - annoyingly forces you to specify some classes as unions rather than using base classes

- added unit conversions
    - currently writing the unit conversion table by hand, fine for now.
    - looked at using popular python packages like pint, unit etc by  few support the 100 in "C/100m"

- added a way to cleanly finsh the pipeline using a special AbortMessage
    - This is necessary for the stateful pipes funtcions like the time aggregator



## Questions
- What's the intended way to use the config file:
    - current setup: config.yaml has sections for sources, preprocessors, aggregators, encoders
    - do we want a config file for canonical variables with output unit, other metadata
- haven't used the plugin code, not sure about the intended use case


## Ideas

## Review meeting in July
Use the mele-mele track to check if the times are being processed correctly

put a bunch of meteotracker data in /perm/ rather than /home/ /HPC_PERM
chunks of data you want to make safe you put in ECFS
store all orginal data

## TODO
- Need to add Coordinate reference systems for lat/lot coords
- investigate the 8 cahracter issue in pyodc
- figure out what offset_tz means in the meteotracker data
- harmonise source, processors and sinks more, could have a diamond inheritance pattern.
- fix the track_id and station translation to use a db rather than a hash
- add a way to distinguish between measurments averaged over a time interval and time point measurements

- dataset@hdr should be longer than 8 characters
make sure aggregator splits tracks accross multiple time_slices


make the meteotracker get_session_data function use the file cache
same for cima
give them an option to use the

- grab as much meteotracker data as possible as raw csv files
- ask cristina about how ECPDS deals with metadata
- will need some way to asign unique track ids

- use docgen or similar to autogenerate documentation for the config from the dataclasses
- Move some of the logic from CSVParser.process to a member function so that you can use the CSVParser directly on DataFrame objects in notebooks.

- added a multiprocess pipeline runner in addition to the single threaded one

data govenerence
add a true unique id
add a proper descriptic label that's longer than 8 bytes

@desc are really the mars keys


add trackid@hdr
stationid@hdr is some alphanumeric unique id per track

Another abstraction on top of configs
Want to dynamically build objects from config so that we can modifty the config online
So might have a parserController that updates its parser factories when the
Look into twisted style wait loops

<!-- parser -->
- match:
    - id: regex

<!-- parsers -->
- match:
    - source: meteotracker
      observation_variable: regex?
    - source: cima
      observation_variable: air_temp

andate and antime should be the time and date of archiving, need to check with Peter about this also anminute!

Show and tell
10 -15 minutes
show a nice graph of tiemsliced data, on a map, multiple ODBs
metadata, data drien

pull requests for pyodc
make it suport pandas string dtime
pull request for suppport for nullable integers
