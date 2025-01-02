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
from typing import Iterable
from unicodedata import normalize

import Levenshtein
import pandas as pd

from ..core.bases import CanonicalVariable, FinishMessage, Mappings, Parser, TabularMessage
from ..core.converters import unit_conversions

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CanonicaliseColumns(Parser):
    """
    Rename columns to canonical variable names and units, and convert their datatypes.

    mappings: dict[original_name -> canonical_name]

    For each data source, mappings.yaml looks like:
      - name: rain
        key: "PLUVIOMETRO [mm]"
        unit: "mm"

    Catesian products will also work:
      - name: equivalent_carbon_dioxide
        key:
            - eco2
            - eCO2
        unit: ["ppm", "ppb"]

    Here any combination of key or unit is allowed.
    
    And there must be a corresponding canonical_variables.yaml entry:
      - name: rain
        unit: "mm"
        dtype: float64

    """
    mappings: Mappings

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

        # CanonicalName -> CanonicalVariable
        self.canonical_variables = {c.name: c for c in globals.canonical_variables}

        # Find the canonical variable that corresponds to each mapping and copy a reference to it
        self.find_canonical_variables()

        # OriginalName -> RawVariable
        # Used for dtype conversions and unit conversions
        self.known_mappings = {c.key : c for c in self.mappings}

        # OriginalName -> CanonicalName
        # Passed to df.rename
        self.rename_dict = {c.key: c.canonical_variable.name for c in self.mappings}


    def find_canonical_variables(self):
        
        for mapping in self.mappings:
            # Find the canonical variable that corresponds to each mapping
            try:
                mapping.canonical_variable = self.canonical_variables[mapping.name]
            except ValueError:
                all = [col for col in self.mappings if col.name not in self.canonical_variables]
                yaml = "".join(
                    f'- name: {col.name}\n  unit: "{col.unit}"\n\n' if col.unit else f"- name: {col.name}\n\n"
                    for col in all
                )
                raise ValueError(
                    f"{mapping.name} from {self} config is not in canonical names!\n"
                    f"Put this into canonical variables:\n{yaml}"
                )
            
            # Emit a warning if the config doesn't include the unit conversion
            # This normalize("NFKD", ...) business is because there are some unicode characters that look identical but have
            # different unicode code points. normalize puts the in some canonical form so they can be compared.
            # Hint: Try out "µ" == "μ"
            if (
                mapping.unit
                and mapping.unit != mapping.canonical_variable.unit
                and f"{mapping.unit.strip()} -> {mapping.canonical_variable.unit.strip()}" not in unit_conversions
            ):
                logger.warning(
                    f"No unit conversion registered for {mapping.name}: {mapping.unit} -> {mapping.canonical_variable.unit}|"
                )

    def convert_units(self, df) -> None:
        # Do unit conversions
        for col in df.columns:
            col = self.known_mappings[col]
            if col.unit != col.canonical_variable.unit:
                if col.canonical_variable.unit is None:
                    raise ValueError(f"{col.canonical_variable=} needs a unit!")
                
                if col.unit is None:
                    raise ValueError(f"{col=} needs a unit!")

                converter = unit_conversions[
                    normalize("NFKD", f"{col.unit.strip()} -> {col.canonical_variable.unit.strip()}")
                ]

                # The conversion is actually the identity function
                if converter is None:
                    continue

                # Apply conversion the underlying numpy array
                df[col.key] = converter(df[col.key].values)

    def format_dataframe(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, CanonicalVariable]]:
        df = df.copy()

        # Check all the columns are actually mapped
        for col in df.columns:
            if col not in self.known_mappings and col not in self.canonical_variables:
                did_you_mean = sorted(self.known_mappings.keys(), key=lambda c: Levenshtein.ratio(col, c))[:3]
                raise ValueError(f"{col} not in CanonicaliseColumns mappings! Did you mean {did_you_mean}?")


        # Do the type conversions
        df = df.astype(dtype = {col : self.known_mappings[col].canonical_variable.dtype for col in df.columns},
                        errors = "raise")
        

        # Do the unit conversions
        self.convert_units(df)

        # Rename the columns
        df.rename(columns=self.rename_dict, inplace=True)
        
        return df, {c : self.canonical_variables[c] for c in df.columns}


    def process(self, rawdata: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(rawdata, FinishMessage):
            return

        df, column_metadata = self.format_dataframe(rawdata.data)

        metadata = self.generate_metadata(
            message=rawdata,
        )

        output_msg = TabularMessage(
            metadata=metadata,
            columns=column_metadata,
            data=df,
        )

        yield self.tag_message(output_msg, rawdata)
