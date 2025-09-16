"""
Ionbeam has to impose it's own 'standards' (to a certain extent) for a DF to be able to read & processed by Ionbeam, these are defined here
"""

from datetime import timedelta

ObservationTimestampColumn = "datetime"
LatitudeColumn = "lat"
LongitudeColumn = "lon"

MaximumCachePeriod = timedelta(days=7) # Defines the TLL for both event + obs data within the Ionbeam system. Ionbeam can only aggregate for a maximum of this period

