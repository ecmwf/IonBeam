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

Recommeneded: Install pre-commit hooks that run ruff, black, isort and flake8 on the code before you commit
```sh
pre-commit install
```

## Testing
To run the default battery of smoke tests, just run pytest:
```sh
$ pytest
```

To run against live APIs use:
```sh
$ pytest -m network
```
