from multiprocessing.sharedctypes import Value
import os

from pathlib import Path
from collections import defaultdict

from hsextract.raster.utils import list_tif_files


def _is_not_hidden_file(path):
    if path.is_dir():
        return False

    is_not_hidden_file = not any(part.startswith('.') for part in path.parts)
    return is_not_hidden_file


def sort_files(include_hidden: bool = False):
    if include_hidden:
        files = [str(p) for p in Path().rglob('*') if not p.is_dir()]
    else:
        files = [str(p) for p in Path().rglob('*') if _is_not_hidden_file(p)]

    sorted_files = sorted(files, key=lambda i: (i, len(i.split("/"))))
    return sorted_files


def categorize_files(files, user_metadata_filename):
    categorized_files = defaultdict(list)

    for f in files:
        if f.endswith(".vrt"):
            categorized_files["raster"].append(f)
        if f.endswith(".tiff"):
            categorized_files["raster-tif"].append(f)
        if f.endswith(".tif"):
            categorized_files["raster-tif"].append(f)

        if f.endswith(".nc"):
            categorized_files["netcdf"].append(f)

        if f.endswith(".shp"):
            categorized_files["feature"].append(f)

        if f.endswith(".refts.json"):
            categorized_files["reftimeseries"].append(f)

        if f.endswith(".csv"):
            categorized_files["timeseries"].append(f)
        if f.endswith(".sqlite"):
            categorized_files["timeseries"].append(f)

        if f.endswith(user_metadata_filename):
            categorized_files["user_meta"].append(f)

    for vrt_file in categorized_files["raster"]:
        vrt_file_dir = os.path.dirname(vrt_file)
        for tif_file in list_tif_files(vrt_file):
            try:
                full_tif_path = os.path.join(vrt_file_dir, tif_file)
                categorized_files["raster-tif"].remove(full_tif_path)
            except ValueError:
                pass  # TODO - warn about an invalid vrt file
    categorized_files["raster"].extend(categorized_files["raster-tif"])
    del categorized_files["raster-tif"]

    return categorized_files


def prepare_files(user_metadata_filename: str):
    sorted_files = sort_files()
    categorized_files = categorize_files(sorted_files, user_metadata_filename)

    return sorted_files, categorized_files
