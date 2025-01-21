
Changed station object location and timespan fields from:
'location': [44.23571, 8.331622],
 'time_span': ['2025-01-02T17:10:00Z', '2025-01-02T17:25:00Z'],
 
 to

'location': {'lat': 44.23571, 'lon': 8.331622},
'time_span': {'start': '2025-01-02T17:10:00Z', 'end': '2025-01-02T17:25:00Z'},