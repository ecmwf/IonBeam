IonBeam
==========

<p align="center">
    <img alt="IonBeam logo, showing a stylized scientific instrument with ion streams." src="_static/ionbeam.png">
</p>

<p align="center">
    <em>⚡️ A library for IoT data wrangling ⚡️</em>
</p>

---


### 📡 What is IonBeam?

**IonBeam** is a toolkit for **stream based processing of IoT observations**. It allows observational IoT data to be 
1. 🔗 Ingested from many sources: REST APIs, MQTT brokers, file servers etc.
2. 🔄 Transformed, cleaned, split, combined and anything else.
3. 💾 Output into multiple storage formats and databases.

Ionbeam uses a **message / action architecture**. Chunks of observations are represented by messages. Messages are routed through a series of actions which perform processing stesps before the final data is written out.

---

### 🚀 Features

- Sources: REST APIs, MQTT brokers, file servers etc.
- Support for both polling and event based data ingestion.
- 
- Transform **streams of messages** with actions before they reach their final destination.
- Compatible with databases or external services for flexible deployment.

---

### 🛠️ Deployment Options

IonBeam can be used in three ways:

1. **Locally**, as a command line tool. See [Command Line Usage](#).
2. **Docker-compose** setup, for local testing and development.
3. (🔜 **Coming Soon**): Deployment via a Heml Chart to Kubernetes cluster.

---

## 💻 Dev Installation

Install from source

```sh
git clone github.com/ecmwf-projects/IonBeam
cd ionbeam
```

Create a conda or mamba environment, venv or similar

```sh
conda env create --name ionbeam ipykernel
conda activate ionbeam
pip install --editable ".[dev]"
```

Recommended: Install pre-commit hooks that run ruff, black, isort, flake8, etc on the code before you commit

```sh
pre-commit install
```

Create a `secrets.yaml` file containing the access credentials for the various sources of data, use `example_secrets.yaml` as a template. `secrets.yaml` is in the gitignore to lower the risk that it accidentally gets committed to git.

## 📚 Documentation

The documentation will be available at on readthedocs.io once this repo is made public. For now you can build it with sphinx.

## 📜 License

[Apache License 2.0](LICENSE) In applying this license, ECMWF does not waive the privileges and immunities
granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.

## ✅ Testing

To run the default battery of smoke tests, just run pytest:

```sh
$ pytest
```

To run against live APIs use:
```sh

$ pytest -m network
```

## 📓 Running the Jupyter notebooks

Setting up a jupyer lab server from scratch using conda or mamba:

```sh
# Make an environment for IonBeam
# Do this at the root of the repository pwd=something/IonBeam/
conda env create --name ionbeam ipykernel
conda activate ionbeam
pip install --editable ".[dev]"

# Make an environment to run jupyter from, using separate ones is best practice
conda env create --name jupyter jupyter nb_conda_kernels 
conda activate jupyter
jupyter lab
```


## 🖥️ Command Line Usage
Currently the main way to interact with the pipeline is through the command line interface.
```bash
% python -m ionbeam --help
usage: IonBeam [-h] [--validate-config] [-v] [-j [NUMBER]]
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



Some example heading
========================

```python
def function(args, **kwargs) -> int:
    return 4
```


.. toctree::
   :maxdepth: 1
   :glob:
   :caption: Table of Contents:

   quickstart
   api_docs
   examples/*


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
