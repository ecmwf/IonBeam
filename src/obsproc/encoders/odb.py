import io

from ..encoders import EncodedData
from ..parsers import ParsedData

try:
    import codc as odc
except ImportError:
    import pyodc as odc


class ODCEncoder:
    def __init__(self, seconds=False, minutes=False):
        self.seconds = seconds
        self.minutes = minutes
        pass

    def __str__(self):
        return "ODCEncoder()"

    def encode(self, data: ParsedData):
        fout = io.BytesIO()

        # Preprocess time fields for odb output

        dt_columns = data.df.select_dtypes(include=["datetimetz", "datetime"])

        # Our remapping modifies columns/structure. But the dataframe could be constructed of sliced dataframes
        # already (see for instance last step of csv.py) - with (e.g.) metadata columns shared with other dfs.
        # So here we ensure it is a proper copy
        df = data.df
        if len(dt_columns) > 0:
            df = df.copy()
            #  What timezone should we be using here?
            # df = tz.normalize(df).astimezone(pytz.utc)

        for colname in dt_columns:
            # @todo Ensure forced to UTC -> dt_columns
            col = df[colname]
            df[f"{colname}date"] = 10000 * col.dt.year + 100 * col.dt.month + col.dt.day
            df[f"{colname}time"] = col.dt.hour
            if self.minutes:
                df[f"{colname}mins"] = col.dt.minutes
            if self.seconds:
                df[f"{colname}secs"] = col.dt.seconds
            del df[colname]

        # And encode the supplied data

        odc.encode_odb(df, fout, properties={"encoded_by": "obsproc"})
        yield EncodedData(format="odb", data=fout.getbuffer(), metadata=data.metadata)


encoder = ODCEncoder
