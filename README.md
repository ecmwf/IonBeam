# IOT Ingester

Install from source
```sh
$ git clone ...
$ cd iot_ingester
```

Create a conda environemnt or similar
```sh
$ conda env create -f conda_env.yml
$ conda activate iot_ingester
```

Install the local files with pip so that the remain editable
```sh
$ pip install --editable ".[dev]"
```