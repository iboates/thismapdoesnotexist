import requests
import pathlib
import os

import docker



regions = {
    # "BW": "https://download.geofabrik.de/europe/germany/baden-wuerttemberg-latest.osm.pbf",
    # "BY": "https://download.geofabrik.de/europe/germany/bayern-latest.osm.pbf",
    # "BE": "https://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf",
    # "BB": "https://download.geofabrik.de/europe/germany/brandenburg-latest.osm.pbf",
    # "HB": "https://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf",
    # "HH": "https://download.geofabrik.de/europe/germany/hamburg-latest.osm.pbf",
    # "HE": "https://download.geofabrik.de/europe/germany/hessen-latest.osm.pbf",
    # "MV": "https://download.geofabrik.de/europe/germany/mecklenburg-vorpommern-latest.osm.pbf",
    # "NI": "https://download.geofabrik.de/europe/germany/niedersachsen-latest.osm.pbf",
    # "NW": "https://download.geofabrik.de/europe/germany/nordrhein-westfalen-latest.osm.pbf",
    # "RP": "https://download.geofabrik.de/europe/germany/rheinland-pfalz-latest.osm.pbf",
    # "SL": "https://download.geofabrik.de/europe/germany/saarland-latest.osm.pbf",
    # "SN": "https://download.geofabrik.de/europe/germany/sachsen-latest.osm.pbf",
    # "ST": "https://download.geofabrik.de/europe/germany/sachsen-anhalt-latest.osm.pbf",
    # "SH": "https://download.geofabrik.de/europe/germany/schleswig-holstein-latest.osm.pbf",
    # "TH": "https://download.geofabrik.de/europe/germany/thueringen-latest.osm.pbf",
    "ANDORRA_TEST": {
        "pbf": "http://download.geofabrik.de/europe/andorra-latest.osm.pbf",
        "poly": "http://download.geofabrik.de/europe/andorra.poly"
    }
}

for region in regions:

    # Launch tileserver container
    docker_client = docker.from_env()
    docker_client.run(
        "overv/openstreetmap-tile-server",
        ports={
            "8081/tcp": 8080,
            "5433/tcp": 5432,
        },
        volumes={
            "openstreetmap-data": {"bind": "/var/lib/postgresql/12/main", "mode": "rw"}
        },
        command="import",
        environment={
            "DOWNLOAD_PBF": regions[region]["pbf"],
            "DOWNLOAD_POLY": regions[region]["poly"]
        }
    )

    # Process tiles into images

    # Delete tileserver container
