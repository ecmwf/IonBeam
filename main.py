import sys

import yaml

from obsproc.core.preprocessing_pipeline import PreprocessingPipelines
from obsproc.encoders import load_encoder
from obsproc.sources import load_source

if __name__ == "__main__":
    # Stages:
    # 1. Raw data from sources (--> location && label)
    # 2. Parsed raw data (into Pandas Dataframes)
    # 3. Annotated raw data --> expand based on decoded values and on ID
    #      --> Self-described messages!!!!
    # 4. Regroup the data
    # 5. Encode output data

    # n.b. Design these such that they can be driven from a configuration database (i.e.
    #      the pre-processing configuration can change dynamically.

    assert len(sys.argv) == 2

    with open(sys.argv[1], "r") as f:
        config = yaml.safe_load(f)

    assert "source" in config

    source = load_source(**config["source"])
    print(source)

    pipelines = PreprocessingPipelines(config["preprocessing"], source)

    # for output in pipelines:
    #     print(f"{output}")
    #     print(output.metadata)
    #     print(output.df)
    #     print(output.df.dtypes)
    #     print('---')
    #     for t in output.df.dtypes:
    #         print(t, type(t))

    # @kodo - this should be treated in the same way as the PreprocessingPipelines --> matches() and multiple options
    encoder = load_encoder(**config["encoder"])

    with open(config["output"], "wb") as fout:
        for parsed in pipelines:
            for encoded in encoder.encode(parsed):
                fout.write(encoded.data)
