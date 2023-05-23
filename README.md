# IOT Ingester

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

## Type Checking
Type checking, currently only CIMA is setup for this.
```sh
mypy src/obsproc/sources/cima
```
