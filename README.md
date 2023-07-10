# IOT Ingester

:warning: This project is ALPHA and will be experimental for the foreseeable future. Interfaces and functionality are likely to change. DO NOT use this software in any project/software that is operational. :warning:

This is a prototype of infrastructure for ingesting IOT observations into the data ECMWF ecosystem.

## Documentation

The documentation will be available at on readthedocs.io once this repo is made public. For now you can build it with sphinx.

## License

[Apache License 2.0](LICENSE) In applying this license, ECMWF does not waive the privileges and immunities
granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.

## Installation

Install from source
```sh
$ git clone github.com/ecmwf-projects/iot-ingester
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

Create a `secrets.yaml` file containing the access credentials for the various sources of data, use `example_secrets.yaml` as a template. `secrets.yaml` is in the gitignore to lower the risk that it accidentally gets committed to git.

## Testing
To run the default battery of smoke tests, just run pytest:
```sh
$ pytest
```

To run against live APIs use:
```sh
$ pytest -m network
```

## Usage
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

### Preprocessor:
- renames columns to canonical names
- formats dates to timezone aware UTC dates
- converts units
- splits data into single variables + metadata columns
- metadata_keys: [`source`, `observed_variable`]

### Aggregators:
- Aggregates data by source, variable and time_slice
- In future could merge different sources if appropriate.
- metadata_keys: [`source`, `observed_variable`, `time_slice`]

### Quality Controlers [to implement]:
- Filters on [`source`, `observed_variable`, `time_slice`], may take more that one unique tuple?
- does some quality control, ML, whatever
- yields output data

### Encoders:
- matches on [`source`, `observed_variable`, `time_slice`]
- encoders to specified output format, currently CSV, ODB

## Changelog

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
