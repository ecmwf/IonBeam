# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

mars_value_formatters = {
    "time": lambda t: f"{t:04d}",
}


def construct_mars_request(message, override=dict(database="fdbdev")):
    source = {"source": message.metadata.filepath}
    odb_keys = {k.key: k.value for k in message.metadata.mars_keys if not k.reason == "Skipped"}
    request = odb_keys | source | override  # rightmost dict takes precedence here
    request = {k: mars_value_formatters.get(k, str)(v) for k, v in request.items()}
    return request
