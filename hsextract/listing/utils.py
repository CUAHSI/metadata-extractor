from pathlib import Path
from collections import defaultdict


def _is_not_hidden_file(path):
    if path.is_dir():
        return False
    
    is_not_hidden_file = not any(part.startswith('.') for part in path.parts)
    return is_not_hidden_file

def sort_files(path: str, include_hidden: bool = False):
    if include_hidden:
        files = [str(p) for p in Path(path).rglob('*') if not path.is_dir()]
    else:
        files = [str(p) for p in Path(path).rglob('*') if _is_not_hidden_file(p)]
    
    sorted_files = sorted(files, key=lambda i: (i, len(i.split("/"))))
    return sorted_files

def categorize_files(files):
    categorized_files = defaultdict(list)

    for f in files:
        #if str(f.name).endswith(".tif"):
        #    categorized_files["raster"].append(f)
        if f.endswith(".vrt"):
            categorized_files["raster"].append(f)
        #if str(f.name).endswith(".tiff"):
        #    categorized_files["raster"].append(f)

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

        if f.endswith("hs_user_meta.json"):
            categorized_files["user_meta"].append(f)

    return categorized_files


def prepare_files(path):
    sorted_files = sort_files(path)
    categorized_files = categorize_files(sorted_files)
    
    return sorted_files, categorized_files
