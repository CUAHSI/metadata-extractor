import asyncio
import os
import json

from hsextract.listing.utils import prepare_files
from hsextract.raster.utils import extract_from_tif_file
from hsextract.feature.utils import extract_metadata_and_files
from hsextract.netcdf.utils import get_nc_meta_dict
from hsextract.reftimeseries.utils import extract_referenced_timeseries_metadata
from hsextract.timeseries.utils import extract_metadata as extract_timeseries_metadata, extract_metadata_csv


def extract_metadata_safe(type: str, filepath):
    try:
        return extract_metadata(type, filepath)
    except Exception as e:
        return filepath, None

def extract_metadata(type: str, filepath):
    extension = os.path.splitext(filepath)[1]
    metadata = None
    if type == "raster":
        metadata = extract_from_tif_file(filepath)
    elif type == "feature":
        metadata = extract_metadata_and_files(filepath)
    elif type == "netcdf":
        metadata = get_nc_meta_dict(filepath)
    elif type == "timeseries":
        if extension == ".csv":
            metadata = extract_metadata_csv(filepath)
        elif extension == ".sqlite":
            metadata = extract_timeseries_metadata(filepath)
    elif type == "reftimeseries":
        metadata = extract_referenced_timeseries_metadata(filepath)

    return filepath, metadata

async def list_and_extract(path: str):
    sorted_files, categorized_files = prepare_files(path)
    tasks = []
    for category, files in categorized_files.items():
        for file in files:
            tasks.append(asyncio.get_running_loop().run_in_executor(None, extract_metadata_safe, category, str(file)))
            #tasks.append(asyncio.to_thread(extract_metadata(category, file)))

    # Will contain a list of dictionaries containing filepath and the extracted metadata
    metadata_list = []
    if tasks:
        metadata_list = await asyncio.gather(*tasks)

    return sorted_files, metadata_list
