import logging
from pathlib import Path
from pathlib import Path
from ionbeam.core.config_parser import parse_config
from ionbeam.core.bases import *
from ionbeam.metadata.db import init_db
from ionbeam.sources.smart_citizen_kit.metadata import construct_sck_metadata

logging.basicConfig(level = logging.INFO)

config_file = Path("~/git/IonBeam-Deployment/config/ionbeam").expanduser()
config, actions = parse_config(config_file,
                    config_path = "./",
                    data_path = "../data/",
                    offline = True,
                    environment  = "local",
                    sources = ["smart_citizen_kit"]
                    )

sck_source = actions[0]
devices = sck_source.get_ICHANGE_devices()

init_db(config.globals)

for device in devices:
    construct_sck_metadata(sck_source, device)