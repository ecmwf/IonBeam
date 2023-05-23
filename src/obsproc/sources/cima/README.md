# CIMA API

This subpackage contains code for reading from the API hosted at https://webdrops.cimafoundation.org/app/
The main sources of data accessible are Acronet weather stations in Northern Italy. See https://www.acronet.cc/

## Example usage:
Make sure there is a `secrets.yaml` file containing your CIMA access credentials, use `example_secrets.yaml` as a template.
Install `obsproc` using the instructions in the root readme.

```python
from obsproc.sources.cima.cima import CIMA_API, GenericSensor, APISensor
from datetime import datetime, timezone, timedelta
from pathlib import Path

cima_api = CIMA_API("path/to/secrets.yaml")

end_date = datetime(2023, 5, 22, hour=0, tzinfo=timezone.utc)
start_date = end_date - timedelta(days=20)

dataframe = cima_api.get_data_by_station(
    station_name="Campo Sportivo Bajardo",
    start_date=start_date,
    end_date=end_date,
    aggregation_time_seconds= 60*60,
)
```
