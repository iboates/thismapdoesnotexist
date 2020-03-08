import math
import csv
import os
from PIL import Image, ImageFilter, ImageStat
import requests
from io import BytesIO
import geopandas as gpd
from functools import partial
import pyproj
from shapely.geometry import Point
from shapely.ops import transform
import shutil


proj_3857_to_4326 = partial(
    pyproj.transform,
    pyproj.Proj('epsg:3857'),
    pyproj.Proj('epsg:4326')
)


def deg2num(lat_deg, lon_deg, zoom):

    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)

    return xtile, ytile


def get_tile_indices(bounds, zoom, step):

    left, lower, right, upper = bounds
    tile_indices = set()
    x_coord = left
    y_coord = lower
    while x_coord < right:
        while y_coord < upper:
            sample_point = Point(x_coord, y_coord)
            sample_point_lonlat = transform(proj_3857_to_4326, sample_point)
            tile_indices.add(deg2num(sample_point_lonlat.x, sample_point_lonlat.y, zoom))
            y_coord += step
        x_coord += step
        y_coord = lower

    return tile_indices


def download_tiles(tile_indices, folder, zoom):
    
    # TODO: use os.path.join instead of simple f strings

    if not os.path.exists(f"{folder}"):
        os.mkdir(f"{folder}")

    for x, y in tile_indices:
        url = f"http://localhost:8081/tile/{zoom}/{x}/{y}.png"
        if not os.path.exists(f"{folder}/{x}/{y}.png"):
            print(f"Requesting '{url}' ...", end="")
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                img = Image.open(BytesIO(r.content))
                img = img.convert("RGB")
                if not os.path.exists(f"{folder}/{y}"):
                    os.mkdir(f"{folder}/{y}")
                img.save(f"{folder}/{y}/{x}.png")  # Save as row-col
                print(" Success.")
            else:
                print(" Failure.")


def stitch_images(folder):

    def _stitch(img1, img2, how):

        if how == "horizontal":

            (width1, height1) = img1.size
            (width2, height2) = img2.size

            stitched_width = width1 + width2
            stitched_height = max(height1, height2)

            stitched = Image.new('RGB', (stitched_width, stitched_height))
            stitched.paste(im=img1, box=(0, 0))
            stitched.paste(im=img2, box=(width1, 0))

        elif how == "vertical":

            (width1, height1) = img1.size
            (width2, height2) = img2.size

            stitched_width = max(width1, width2)
            stitched_height = height1 + height2

            stitched = Image.new('RGB', (stitched_width, stitched_height))
            stitched.paste(im=img1, box=(0, height1))
            stitched.paste(im=img2, box=(0, height2))

        else:
            raise ValueError("Valid 'how' types are 'horizontal' and 'vertical'")

        return stitched

    stitched_rows = []
    for row in sorted(os.listdir(folder)):
        cols = sorted(os.listdir(f"{folder}/{row}"))
        for i in range(len(cols) - 1):
            if i == 0:
                stitched = Image.open(f"{folder}/{row}/{cols[i]}")
            img2 = Image.open(f"{folder}/{row}/{cols[i + 1]}")
            stitched = _stitch(stitched, img2, "horizontal")
        stitched_rows.append(stitched)

    for i in range(len(stitched_rows)):
        stitched_rows[i] = stitched_rows[i].rotate(90, expand=True)
    for i in range(len(stitched_rows) - 1):
        if i == 0:
            stitched = stitched_rows[i]
        img2 = stitched_rows[i + 1]
        stitched = _stitch(stitched, img2, "horizontal")
    stitched = stitched.rotate(270, expand=True)

    return stitched


def centre_crop_image(img):

    # Crop the image to a fixed size from the centre
    # We want about squares of about 1000m on each side
    crop_width, crop_height = window_length / m_per_pixel[zoom], window_length / m_per_pixel[zoom]
    width, height = stitched_img.size
    left = (width - crop_width) / 2
    top = (height - crop_height) / 2
    right = (width + crop_width) / 2
    bottom = (height + crop_height) / 2

    # Crop the center of the image
    cropped_img = stitched_img.crop((left, top, right, bottom))

    return cropped_img


# zoom = 16
# m_per_pixel = {  # From https://wiki.openstreetmap.org/wiki/Zoom_levels
#     0: 156412,
#     1: 78206,
#     2: 39103,
#     3: 19551,
#     4: 9776,
#     5: 4888,
#     6: 2444,
#     7: 1222,
#     8: 610.984,
#     9: 305.492,
#     10: 152.746,
#     11: 76.373,
#     12: 38.187,
#     13: 19.093,
#     14: 9.547,
#     15: 4.773,
#     16: 2.387,
#     17: 1.193,
#     18: 0.596,
#     19: 0.298,
#     20: 0.149
# }
# window_length = 1000
# step = 100
# gdf = gpd.GeoDataFrame.from_file("data/de/locations/nordrhein_westfalen_villages_towns.gpkg")
# gdf_len = len(gdf.index)
#
# for i, row in gdf.iterrows():
#
#     feature = row["geometry"]
#
#     # Get tile indices associated with this point
#     lower, upper = feature.y - (window_length / 2), feature.y + (window_length / 2)
#     left, right = feature.x - (window_length / 2), feature.x + (window_length / 2)
#     tile_indices = get_tile_indices(lower, upper, left, right)
#
#     # Clean out working dir
#     try:
#         shutil.rmtree("data/_temp")
#         os.mkdir("data/_temp")
#     except:
#         pass
#
#     # Download all the tiles
#     download_tiles(tile_indices, "data/_temp", zoom)
#
#     # Stich together the tiles
#     stitched_img = stitch_images("data/_temp")
#
#     # Crop the image down to a standard size
#     cropped_img = centre_crop_image(stitched_img)
#
#     cropped_img.save(f"data/de/tiles/{str(i).zfill(9)}.png")
#
#     if i % 100 == 0:
#         print(f"Finished {i}/{gdf_len}.")

