import requests
import pathlib
import os
import time

import docker
import geopandas as gpd
import psycopg2 as pg2

from download_tiles import *


regions = {
    # "BW": "https://download.geofabrik.lx/europe/germany/baden-wuerttemberg-latest.osm.pbf",
    # "BY": "https://download.geofabrik.lx/europe/germany/bayern-latest.osm.pbf",
    # "BE": "https://download.geofabrik.lx/europe/germany/berlin-latest.osm.pbf",
    # "BB": "https://download.geofabrik.lx/europe/germany/brandenburg-latest.osm.pbf",
    # "HB": "https://download.geofabrik.lx/europe/germany/bremen-latest.osm.pbf",
    # "HH": "https://download.geofabrik.lx/europe/germany/hamburg-latest.osm.pbf",
    # "HE": "https://download.geofabrik.lx/europe/germany/hessen-latest.osm.pbf",
    # "MV": "https://download.geofabrik.lx/europe/germany/mecklenburg-vorpommern-latest.osm.pbf",
    # "NI": "https://download.geofabrik.lx/europe/germany/niedersachsen-latest.osm.pbf",
    # "NW": "https://download.geofabrik.lx/europe/germany/nordrhein-westfalen-latest.osm.pbf",
    # "RP": "https://download.geofabrik.lx/europe/germany/rheinland-pfalz-latest.osm.pbf",
    # "SL": "https://download.geofabrik.lx/europe/germany/saarland-latest.osm.pbf",
    # "SN": "https://download.geofabrik.lx/europe/germany/sachsen-latest.osm.pbf",
    # "ST": "https://download.geofabrik.lx/europe/germany/sachsen-anhalt-latest.osm.pbf",
    # "SH": "https://download.geofabrik.lx/europe/germany/schleswig-holstein-latest.osm.pbf",
    # "TH": "https://download.geofabrik.lx/europe/germany/thueringen-latest.osm.pbf",
    "ANDORRA_TEST": {
        "pbf": "http://download.geofabrik.lx/europe/andorra-latest.osm.pbf",
        "poly": "http://download.geofabrik.lx/europe/andorra.poly"
    }
}

client = docker.from_env()
zoom = 16
m_per_pixel = {  # From https://wiki.openstreetmap.org/wiki/Zoom_levels
    0: 156412,
    1: 78206,
    2: 39103,
    3: 19551,
    4: 9776,
    5: 4888,
    6: 2444,
    7: 1222,
    8: 610.984,
    9: 305.492,
    10: 152.746,
    11: 76.373,
    12: 38.187,
    13: 19.093,
    14: 9.547,
    15: 4.773,
    16: 2.387,
    17: 1.193,
    18: 0.596,
    19: 0.298,
    20: 0.149
}
step = 100

for region in regions:

    try:

        # Launch tileserver container
        import_container = client.containers.run(
            "overv/openstreetmap-tile-server",
            ports={
                "80/tcp": 8081
            },
            volumes={
                "openstreetmap-data": {"bind": "/var/lib/postgresql/12/main", "mode": "rw"}
            },
            command="import",
            environment={
                "DOWNLOAD_PBF": regions[region]["pbf"],
                "DOWNLOAD_POLY": regions[region]["poly"]
            },
            auto_remove=True
        )
        tile_server = client.containers.run(
            "overv/openstreetmap-tile-server",
            name=f"tileserver_{region}",
            ports={
                "80/tcp": 8081,
                "5432/tcp": 5433,
            },
            volumes={
                "openstreetmap-data": {"bind": "/var/lib/postgresql/12/main", "mode": "rw"}
            },
            command="run",
            environment={
                "ALLOW_CORS": "enabled"
            },
            detach=True
        )
        # TODO: need to get a signal that the container is ready to be contacted, this is a cheap workaround
        time.sleep(10)

        # Process tiles into images
        with pg2.connect(host="localhost", port=5433, dbname="gis", user="renderer", password="renderer") as conn:
            locations = gpd.GeoDataFrame.from_postgis(
                "SELECT St_Envelope(ST_Buffer(way, 500)) AS geom FROM planet_osm_point WHERE place IN ('town', 'village')",
                conn,
                crs="EPSG:3857"
            )

        for i, row in locations.iterrows():

            feature = row["geom"]
            tile_indices = get_tile_indices(feature.bounds, zoom, step)

            # Clean out working dir
            try:
                shutil.rmtree("data/_temp")
                os.mkdir("data/_temp")
            except:
                pass

            # Download all the tiles
            download_tiles(tile_indices, "data/_temp", zoom)

            # Stich together the tiles
            stitched_img = stitch_images("data/_temp")

            # Crop the image down to a standard size
            # cropped_img = centre_crop_image(stitched_img)

            # cropped_img.save(f"data/lx/tiles/{str(i).zfill(9)}.png")
            stitched_img.save(f"data/lx/tiles/{str(i).zfill(9)}.png")

    finally:
        # Delete tileserver container
        tile_server.stop()
        tile_server.remove()
