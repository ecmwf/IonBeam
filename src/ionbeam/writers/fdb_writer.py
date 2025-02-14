# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import dataclasses
import logging
import os
import shutil
from pathlib import Path
from time import time
from typing import Iterable

import findlibs
import yaml
from jinja2 import Template

from ..core.bases import BytesMessage, DataMessage, FileMessage, Message, Writer
from ..core.time import fmt_time

logger = logging.getLogger(__name__)

def install_metkit_overlays(template, keys):
    # Find the location of the metkit library
    metkit_path = findlibs.find("metkit")
    if not metkit_path:
        raise ValueError("Failed to find metkit library")

    # Check the path conforms to our expectations
    if Path(metkit_path).with_suffix("").parts[-2:] == ('lib', 'libmetkit'):
        base = Path(metkit_path).parents[1]
    else:
        # raise ValueError(f"Unexpected metkit location: {metkit_path}")
        base = Path("/usr/local")

    # Figure out the location of the language.yaml and odb/marsrequest.yaml files
    language_path = base / "share/metkit/language.yaml"
    request_path = base / "share/metkit/odb/marsrequest.yaml"
    
    if not language_path.exists() or not request_path.exists():
        raise ValueError(f"Tried to overide the metkit yaml files can't find one of them at {base}.")

    def backup(src):
        dst = src.with_suffix(".yaml.original")
        if not dst.exists():
            logger.debug(f"Copying backup to {dst}")
            shutil.copyfile(src, dst)
        else:
            logger.debug(f"{src.name} backup exists at {dst}")
    
    backup(language_path)
    backup(request_path)
    
    with open(template) as f:
        template = Template(f.read())  

    existing_keys = yaml.safe_load(template.render(keys_yaml = ""))["_field"].keys()
    keys_yaml = yaml.dump(
        {k : {
                "category": "data",
                "type": "any"
            }
        for k in keys if k not in existing_keys})

    logger.debug(f"Writing language.yaml overlay to {language_path}")
    with open(language_path, "w") as f:
        f.write(template.render(keys_yaml = keys_yaml))

    logger.debug(f"Writing marsrequest.yaml overlay to {request_path}")

    with open(request_path, "w") as f:
        f.write("---\n" + "\n".join(
            f"{k.upper()}: {k.lower()}"
            for k in keys
        ))

@dataclasses.dataclass
class FDBWriter(Writer):
    config: dict | None = None
    debug: list[str] = dataclasses.field(default_factory=list)

    def __str__(self):
        return f"{self.__class__.__name__}()"

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        self.set_metadata = dataclasses.replace(self.set_metadata, state="written")
        self.metkit_language_template = globals.metkit_language_template

        if not self.config:
            self.config = dict(
                engine = "toc",
                spaces = [
                    dict(
                        handler = "Default",
                        roots = [
                            dict(
                                path = str(globals.fdb_root)
                            )
                        ]
                    )
                ]
            )

        if "schema" not in self.config:
            self.config["schema"] = str(globals.fdb_schema_path)

        self.fdb = self.configure_fdb_client()

    def configure_fdb_client(self):
        fdb5_path = findlibs.find("fdb5")
        logger.debug(f"FDBWriter using fdb5 shared library from {fdb5_path}")

        for lib in self.debug:
            os.environ[f"{lib.upper()}_DEBUG"] = "1"

        logger.debug("Installing metkit overlays")
        install_metkit_overlays(self.metkit_language_template, self.globals.custom_mars_keys)

        import pyfdb
        
        return pyfdb.FDB(self.config)

    def process(self, input_message: FileMessage | BytesMessage) -> Iterable[Message]:

        
        assert input_message.metadata.mars_id is not None
        mars_id = input_message.metadata.mars_id.as_strings()

        t0 = time()
        existing_matches = list(self.fdb.list(mars_id))
        n = len(existing_matches)
        logger.debug(f"Found {n} existing matches for {mars_id} in {fmt_time(time() - t0)}")

        if n == 0:
            t0 = time()
            self.fdb.archive_single(input_message.data_bytes(), mars_id)
            self.fdb.flush()
            logger.debug(f"Added new data for {mars_id} in {fmt_time(time() - t0)}")

        elif not self.globals.overwrite_fdb:
            logger.debug("Dropping data because it's already in the database.")
        
        elif n == 1:
            t0 = time()
            logger.debug(f"Overwriting existing data for {mars_id}")
            path = existing_matches[0]["path"]
            logger.debug(f"Overwriting data at {path}")
            with open(path, "wb") as f:
                f.seek(0)
                f.truncate()
                f.write(input_message.data_bytes())
            logger.debug(f"Overwrote data for {mars_id} in {fmt_time(time() - t0)}")

        elif n > 1:
            raise ValueError(f"Multiple matches for {mars_id} in the database.")
        
        metadata = self.generate_metadata(input_message)
        output_msg = DataMessage(metadata=metadata)
        yield output_msg