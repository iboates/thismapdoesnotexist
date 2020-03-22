from PIL import Image
import os

for bl in [
    "BB",
    "BE",
    "BW",
    "BY",
    "HB",
    "HE",
    "HH",
    "MV",
    "NI",
    "NW",
    "RP",
    "SH",
    "SL",
    "SN",
    "ST",
    "TH",
]:
    print(bl)
    for img_path in os.listdir(f"data/de/{bl}/tiles"):
        img = Image.open(f"data/de/{bl}/tiles/{img_path}")
        img.thumbnail((256, 256))
        img.save(f"data/de/256/{bl}{img_path}", "PNG")