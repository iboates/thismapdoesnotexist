import time

import docker
import psycopg2 as pg2

from download_tiles import *


regions = {
    # "BW": {"pbf": "https://download.geofabrik.de/europe/germany/baden-wuerttemberg-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/baden-wuerttemberg.poly"},
    # "BY": {"pbf": "https://download.geofabrik.de/europe/germany/bayern-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/bayern.poly"},
    # "BE": {"pbf": "https://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/berlin.poly"},
    # "BB": {"pbf": "https://download.geofabrik.de/europe/germany/brandenburg-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/brandenburg.poly"},
    # "HB": {"pbf": "https://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/bremen.poly"},
    # "HH": {"pbf": "https://download.geofabrik.de/europe/germany/hamburg-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/hamburg.poly"},
    # "HE": {"pbf": "https://download.geofabrik.de/europe/germany/hessen-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/hesse.poly"},
    # "MV": {"pbf": "https://download.geofabrik.de/europe/germany/mecklenburg-vorpommern-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/mecklenburg-vorpommern.poly"},
    # "NI": {"pbf": "https://download.geofabrik.de/europe/germany/niedersachsen-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/niedersachsen.poly"},
    # "NW": {"pbf": "https://download.geofabrik.de/europe/germany/nordrhein-westfalen-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/nordrhein-westfalen.poly"},
    # "RP": {"pbf": "https://download.geofabrik.de/europe/germany/rheinland-pfalz-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/rheinland-pfalz.poly"},
    "SL": {"pbf": "https://download.geofabrik.de/europe/germany/saarland-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/saarland.poly"},
    # "SN": {"pbf": "https://download.geofabrik.de/europe/germany/sachsen-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/sachsen.poly"},
    # "ST": {"pbf": "https://download.geofabrik.de/europe/germany/sachsen-anhalt-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/sachsen-anhalt.poly"},
    # "SH": {"pbf": "https://download.geofabrik.de/europe/germany/schleswig-holstein-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/schleswig-holstein.poly"},
    # "TH": {"pbf": "https://download.geofabrik.de/europe/germany/thueringen-latest.osm.pbf", "poly": "http://download.geofabrik.de/europe/thueringen.poly"},
#     "LX_TEST": {"pbf": "https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/luxembourg.poly"}
}

# fr_regions = {
# "Alsace": {"pbf": "https://download.geofabrik.de/europe/france/alsace-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/alsace.poly"},
# "Aquitaine": {"pbf": "https://download.geofabrik.de/europe/france/aquitaine-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/aquitaine.poly"},
# "Auvergne": {"pbf": "https://download.geofabrik.de/europe/france/auvergne-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/auvergne.poly"},
# "Basse-Normandie": {"pbf": "https://download.geofabrik.de/europe/france/basse-normandie.html", "poly": "https://download.geofabrik.de/europe/france/basse.poly"},
# "Bourgogne": {"pbf": "https://download.geofabrik.de/europe/france/bourgogne-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/bourgogne.poly"},
# "Bretagne": {"pbf": "https://download.geofabrik.de/europe/france/bretagne-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/bretagne.poly"},
# "Centre": {"pbf": "https://download.geofabrik.de/europe/france/centre-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/centre.poly"},
# "Champagne-Ardenne": {"pbf": "https://download.geofabrik.de/europe/france/champagne-ardenne-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/champagne-ardenne.poly"},
# "Corse": {"pbf": "https://download.geofabrik.de/europe/france/corse-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/corse.poly"},
# "Franche-Compte": {"pbf": "https://download.geofabrik.de/europe/france/franche-comte-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/franche-comte.poly"},
# "Haute-Normandie": {"pbf": "https://download.geofabrik.de/europe/france/haute-normandie-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/haute-normandie.poly"},
# "Ile-De-France": {"pbf": "https://download.geofabrik.de/europe/france/ile-de-france-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/ile-de-france.poly"},
# "Languedoc-Roussillon": {"pbf": "https://download.geofabrik.de/europe/france/languedoc-roussillon-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/languedoc-roussillon.poly"},
# "Limousin": {"pbf": "https://download.geofabrik.de/europe/france/limousin-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/limousin.poly"},
# "Lorraine": {"pbf": "https://download.geofabrik.de/europe/france/lorraine-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/lorraine.poly"},
# "Midi-Pyrenees": {"pbf": "https://download.geofabrik.de/europe/france/midi-pyrenees-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/midi-pyrenees.poly"},
# "Nord-Pas-De-Calais": {"pbf": "https://download.geofabrik.de/europe/france/nord-pas-de-calais-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/nord-pas-de-calais.poly"},
# "Pays-De-La-Loire": {"pbf": "https://download.geofabrik.de/europe/france/pays-de-la-loire-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/pays-de-la-loire.poly"},
# "Picardie": {"pbf": "https://download.geofabrik.de/europe/france/picardie-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/picardie.poly"},
# "Poitou-Charentes": {"pbf": "https://download.geofabrik.de/europe/france/poitou-charentes-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/poitou-charentes.poly"},
# "Provence-Alpes-Cote-D-Azur": {"pbf": "https://download.geofabrik.de/europe/france/provence-alpes-cote-d-azur-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/provence-alpes-cote-d-azur.poly"},
# "Rhone-Alpes": {"pbf": "https://download.geofabrik.de/europe/france/rhone-alpes-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/rhone-alpes.poly"},
# "LX_TEST": {"pbf": "https://download.geofabrik.de/europe/france/luxembourg-latest.osm.pbf", "poly": "https://download.geofabrik.de/europe/france/luxembourg.poly"}
# }


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
tilesize = 256
query_file = "sql/villages_towns_suburbs_with_more_than_50_buildings_nearby.sql"
out_folder = "data/de/train"

for region in regions:

    print(f"{time.strftime('%H:%M:%S', time.localtime())}: Processing {region}... ")

    try:
        os.mkdir(f"data/de/{region}")
        os.mkdir(f"data/de/{region}/tiles")
    except:
        pass

    try:

        # Launch tileserver container
        print(f"{time.strftime('%H:%M:%S', time.localtime())}: Building tileserver... ")
        import_container = client.containers.run(
            "overv/openstreetmap-tile-server:latest",
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
        print(f"{time.strftime('%H:%M:%S', time.localtime())}: Import finished. ")
        print(f"{time.strftime('%H:%M:%S', time.localtime())}: Running tileserver... ")
        tile_server = client.containers.run(
            "overv/openstreetmap-tile-server:latest",
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
        print(f"{time.strftime('%H:%M:%S', time.localtime())}: Tileserver finished. ")
        print(f"{time.strftime('%H:%M:%S', time.localtime())}: Extracting images... ")

        # Process tiles into images
        with pg2.connect(host="localhost", port=5433, dbname="gis", user="renderer", password="renderer") as conn:
            locations = gpd.GeoDataFrame.from_postgis(
                open(query_file).read(),
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
            cropped_img = centre_crop_image(stitched_img, tilesize)

            cropped_img.save(f"{out_folder}/{region}{str(i).zfill(9)}.png")

            if i % 100 == 0:
                print(f"{time.strftime('%H:%M:%S', time.localtime())}: Finished image {i}/{len(locations.index)}. ")

        print(f"{time.strftime('%H:%M:%S', time.localtime())}: Finished image {region}.")

    finally:
        # Delete tileserver containers
        try:
            tile_server.stop()
        except:
            pass
        try:
            tile_server.remove()
        except:
            pass
        # pass

    print(f"{time.strftime('%H:%M:%S', time.localtime())}: Done.")
