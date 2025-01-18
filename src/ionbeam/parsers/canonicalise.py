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
from typing import Callable, Iterable, Sequence
from unicodedata import normalize

import Levenshtein
import pandas as pd

from ..core.bases import CanonicalVariable, Mappings, Parser, RawVariable, TabularMessage
from ..core.converters import unit_conversions

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class ComputeColumnMappingsByName(Parser):
    """Given the mappings, compute the column mappings based purely on the name of each column"""
    mappings: Mappings
    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        self.mappings.link(globals.canonical_variables)
        self.column_mappings_by_key = {c.key : c for c in self.mappings}

    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        for col in msg.data.columns:
            if col in msg.metadata.columns:
                continue # already mapped
            elif col in self.column_mappings_by_key:
                msg.metadata.columns[col] = self.column_mappings_by_key[col]
            else:
                logger.warning(f"ComputeColumnMappingsByName: Column {col} not in mappings")
        yield msg

@dataclasses.dataclass
class ConvertDtypes(Parser):
    """Calls msg.data.convert_dtypes()"""
    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        msg.data = msg.data.convert_dtypes()
        yield msg

@dataclasses.dataclass
class FormatChecks(Parser):
    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        assert isinstance(msg.data, pd.DataFrame)
        assert len(set(msg.data.columns)) == len(msg.data.columns), f"Duplicate columns in dataframe {msg.data.columns}"
        assert isinstance(msg.data.index, pd.DatetimeIndex)
        assert msg.data.index.name == "datetime"

        expected_columns = [
            "external_station_id",
            "station_id",
            "lat",
            "lon",
        ]
        for col in expected_columns:
            assert col in msg.data.columns, f"{col} not in msg.data.columns"

        only_meta = [column for column in msg.data.columns if column not in msg.metadata.columns]
        assert not only_meta, f"{only_meta} in msg.data.columns but not in msg.metadata.columns"

        only_data = [column for column in msg.metadata.columns if column not in msg.data.columns]
        assert not only_data, f"{only_data} in msg.metadata.columns but not in msg.data.columns"
            
        yield msg

@dataclasses.dataclass
class DataFrameChanges:
    discard_columns: Sequence[str]
    dtype_conversions: dict[str, str]
    unit_conversions: dict[str, Callable[[pd.Series], pd.Series]]
    rename_columns: dict[str, str]

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
    move_to_front: list[str] = dataclasses.field(default_factory=list)
    to_discard: set[str] = dataclasses.field(default_factory=set, init=False)

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

        # CanonicalName -> CanonicalVariable
        self.canonical_variables = {c.name: c for c in globals.canonical_variables}

        # Find the canonical variable that corresponds to each mapping and copy a reference to it
        self.to_discard = set()
        self.find_canonical_variables()

        # OriginalName -> RawVariable
        # Used for dtype conversions and unit conversions
        self.known_mappings = {c.key : c for c in self.mappings}

        # OriginalName -> CanonicalName
        # Passed to df.rename
        self.rename_dict = {c.key: c.canonical_variable.name for c in self.mappings}

        self.dtype_map = {c.key : c.canonical_variable.dtype for c in self.mappings}


    def find_canonical_variables(self):
        new_mappings = []
        for mapping in self.mappings:
            # Find the canonical variable that corresponds to each mapping
            if mapping.discard:
                self.to_discard.add(mapping.key)
                continue
            try:
                mapping.canonical_variable = self.canonical_variables[mapping.name]
            except KeyError:
                all = [col for col in self.mappings if col.name not in self.canonical_variables]
                yaml = "".join(
                    f'- name: {col.name}\n  unit: "{col.unit}"\n\n' if col.unit else f"- name: {col.name}\n\n"
                    for col in all if not col.discard
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
            ):
                    
                if mapping.unit is None:
                    raise ValueError(f"{mapping = } needs a unit!")

                if mapping.canonical_variable.unit is None:
                    raise ValueError(f"{mapping.name=} needs a unit!")
            

                try:
                    mapping.converter = unit_conversions[
                        normalize("NFKD", f"{mapping.unit.strip()} -> {mapping.canonical_variable.unit.strip()}")
                    ]
                except KeyError:
                    logger.warning(
                        f"No unit conversion registered for {mapping.name}: {mapping.unit} -> {mapping.canonical_variable.unit}|"
                    )
            else: 
                mapping.converter = None
            new_mappings.append(mapping)
        
        self.mappings = new_mappings

    def complain_about_unmapped(self, unmapped : set[str]):
        msg = []
        for col in unmapped:
            did_you_mean = sorted(self.known_mappings.keys(), key=lambda c: Levenshtein.ratio(col, c))[:3]
            msg.append(f"{col} not in CanonicaliseColumns mappings! Did you mean {did_you_mean}?")
        raise ValueError("\n".join(msg))

    def compute_changes_from_column_names(self, df : pd.DataFrame) -> DataFrameChanges:
        # Check all the columns are actually mapped
        unmapped = set(df.columns) - set(c.key for c in self.known_mappings.values()) - set(self.canonical_variables.keys())
        if unmapped:
            self.complain_about_unmapped(unmapped)

        return DataFrameChanges(
            discard_columns = [col for col in df.columns if col in self.to_discard],
            dtype_conversions = {c : v for c, v in self.dtype_map.items() if c in df.columns and c not in self.to_discard},
            unit_conversions = {c : v.converter for c, v in self.known_mappings.items() if v.converter is not None and c not in self.to_discard},
            rename_columns = self.rename_dict,
        )
    
    def compute_changes_from_raw_variables(self, df : pd.DataFrame, columns: dict[str, RawVariable | CanonicalVariable]) -> DataFrameChanges:
            unmapped = set(df.columns) - set(col.key for col in columns.values()) - set(self.canonical_variables.keys())
            if unmapped:
                self.complain_about_unmapped(unmapped)

            all_columns = list(columns.values())
            raw_columns = [col for col in columns.values() if isinstance(col, RawVariable)]
            canonical_columns = [col for col in columns.values() if isinstance(col, CanonicalVariable)]

            raw_dtype_conversions = {c.key : c.canonical_variable.dtype for c in raw_columns if c.canonical_variable is not None and c.canonical_variable.dtype is not None}
            canonical_dtype_conversions = {c.key : c.dtype for c in canonical_columns if c.dtype is not None}

            return DataFrameChanges(
                discard_columns = [col.key for col in all_columns if col.discard] + list(unmapped),
                dtype_conversions = raw_dtype_conversions | canonical_dtype_conversions,
                unit_conversions = {c.key : c.converter for c in raw_columns if c.converter is not None},
                rename_columns = {c.key: c.name for c in raw_columns},
            )

    def format_dataframe(self, 
                    df: pd.DataFrame,
                    columns: dict[str, RawVariable | CanonicalVariable] | None,
                    ) -> tuple[pd.DataFrame, dict[str, CanonicalVariable]]:
        # df = df.copy()
        
        # For most sources it is sufficient to look at the column name to decide how to canonicalise it
        # For Smart Citizen Kit however we need to know at least the name and the unit, so instead we pass in the raw variables from the source
        changes = self.compute_changes_from_column_names(df) \
                if not columns else self.compute_changes_from_raw_variables(df, columns)


        # Remove columns that are marked as discard
        df.drop(columns=changes.discard_columns, inplace=True)

        # Convert the datatypes
        df = df.astype(dtype = changes.dtype_conversions, errors = "raise")

        # Convert units
        for name, converter in changes.unit_conversions.items():
            if name not in df.columns:
                logger.debug(f"Column {name} not in dataframe")
                continue
            df[name] = converter(df[name])

        # Rename the columns
        df.rename(columns = changes.rename_columns, inplace=True)

        # reorder the columns
        front_cols = []
        for col in self.move_to_front:
            if col in df.columns:
                front_cols.append(col)

        other_cols = [c for c in df.columns if c not in front_cols]
        final_cols = front_cols + other_cols
        df = df[final_cols]

        if not columns:
            new_columns = {c : self.canonical_variables[c] for c in df.columns}
        else: 
            filtered_columns = [c for c in columns.values() if c.name in df.columns]
            new_columns = [c.make_canonical() if isinstance(c, RawVariable) else c
                          for c in filtered_columns]
            new_columns = {c.name : c for c in new_columns}
        
        df.convert_dtypes()

        return df, new_columns

    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        df, column_metadata = self.format_dataframe(msg.data, columns = msg.metadata.columns)
        msg.metadata.columns = column_metadata
        msg.data = df
        yield msg