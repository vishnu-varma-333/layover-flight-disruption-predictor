"""The fixed set of demo airports live ingestion monitors.

Hardcoded rather than read from data/raw/reference/airports.csv (the
Milestone 2 download, which is gitignored and not guaranteed present when
an ingestor starts): these 15 major US hubs change coordinates rarely
enough that pinning them is reasonable, and it removes a hard runtime
dependency on a multi-hundred-MB file an ingestor has no other reason to
need. Coordinates sourced from that same OurAirports reference data, not
guessed.

Scope matches the spec's own "targeted polling, not the whole globe"
design consequence for OpenSky's credit-limited API (see DECISIONS.md) --
these airports anchor every ingestor's monitored set, not just OpenSky's.
"""

MONITORED_AIRPORTS = [
    {"iata": "ATL", "icao": "KATL", "lat": 33.6367, "lon": -84.428101},
    {"iata": "BOS", "icao": "KBOS", "lat": 42.36197, "lon": -71.0079},
    {"iata": "CLT", "icao": "KCLT", "lat": 35.214001, "lon": -80.9431},
    {"iata": "DEN", "icao": "KDEN", "lat": 39.860027, "lon": -104.673792},
    {"iata": "DFW", "icao": "KDFW", "lat": 32.896801, "lon": -97.038002},
    {"iata": "IAH", "icao": "KIAH", "lat": 29.9844, "lon": -95.3414},
    {"iata": "JFK", "icao": "KJFK", "lat": 40.639447, "lon": -73.779317},
    {"iata": "LAS", "icao": "KLAS", "lat": 36.083361, "lon": -115.151817},
    {"iata": "LAX", "icao": "KLAX", "lat": 33.942501, "lon": -118.407997},
    {"iata": "MCO", "icao": "KMCO", "lat": 28.4294, "lon": -81.308998},
    {"iata": "MIA", "icao": "KMIA", "lat": 25.796011, "lon": -80.289751},
    {"iata": "ORD", "icao": "KORD", "lat": 41.9786, "lon": -87.9048},
    {"iata": "PHX", "icao": "KPHX", "lat": 33.435302, "lon": -112.005905},
    {"iata": "SEA", "icao": "KSEA", "lat": 47.447943, "lon": -122.310276},
    {"iata": "SFO", "icao": "KSFO", "lat": 37.619806, "lon": -122.374821},
]
