from datetime import timedelta

# Standard column names required by ionbeam
ObservationTimestampColumn = "datetime"
LatitudeColumn = "lat"
LongitudeColumn = "lon"

# Maximum cache period for ionbeam system
MaximumCachePeriod = timedelta(days=7)
