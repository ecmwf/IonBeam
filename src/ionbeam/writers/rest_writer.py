# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

from typing import Iterable
from pathlib import Path

import pandas as pd

import dataclasses

from ..core.bases import Writer, Message, FileMessage, FinishMessage

import logging
import findlibs
import os
import yaml
import json
from jinja2 import Template
import shutil

logger = logging.getLogger(__name__)

import yaml
import pyodc as odc
from io import BytesIO
from dataclasses import dataclass
import requests
import json

@dataclass
class IonBeamAPI:
    endpoint : str
    auth_token: str | None = None

    def __post_init__(self):
        self.endpoint = self.endpoint.rstrip("/")
        self.session = requests.Session()
        if self.auth_token: 
            self.session.headers["Authorization"] = f"Bearer {self.auth_token}"

    def get(self, path : str, **kwargs) -> requests.Response:
        return self.session.get(f"{self.endpoint}/{path}", stream=True, **kwargs)   
    
    def get_json(self, path, **kwargs):
        resp = self.get(path, **kwargs)
        try:
            return resp.json()
        except json.JSONDecodeError:
            logger.warning(resp.text)
            return resp.text
            
    
    def list(self, request : dict[str, str] = {}):
        return self.get_json("list", params = request)
    
    def head(self, request : dict[str, str] = {}):
        return self.get_json("head", params = request)

    def stations(self, **kwargs):
        return self.get_json("stations", params = kwargs)

    def archive(self, file, request = dict()) -> requests.Response:
        files = {'file': file}
        return self.session.post(f"{self.endpoint}/archive",
                                 files=files, 
                                 params = request)


@dataclasses.dataclass
class RESTWriter(Writer):

    def __str__(self):
        return f"{self.__class__.__name__}()"

    def init(self, globals):
        super().init(globals)
        assert self.globals is not None
        assert self.globals.secrets is not None
        assert self.globals.api_hostname is not None
        self.api = IonBeamAPI(endpoint = self.globals.api_hostname, auth_token = self.globals.secrets["polytope"]["user_key"])
        

    def process(self, input_message: FileMessage | FinishMessage) -> Iterable[Message]:
        if isinstance(input_message, FinishMessage):
            return

        assert input_message.metadata.filepath is not None

        request = input_message.metadata.mars_request.as_strings()
        
        if len(self.api.list(request)) > 0 and not self.globals.overwrite:
            logger.debug(f"Dropping data because it's already in the database.")
        else:
            with open(input_message.metadata.filepath, "rb") as f:
                resp = self.api.archive(f, request = request)
                resp.raise_for_status()
                logger.debug(resp.json())
                logger.info(f"Archiving to {self.globals.api_hostname}")

        metadata = self.generate_metadata(input_message, mars_request=input_message.metadata.mars_request)
        output_msg = FileMessage(metadata=metadata)
        yield self.tag_message(output_msg, input_message)
