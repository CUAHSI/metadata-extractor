import asyncio
import os
import json

from hsextract.listing.utils import prepare_files
from hsextract.raster.utils import extract_from_tif_file
from hsextract.feature.utils import extract_metadata_and_files
from hsextract.netcdf.utils import get_nc_meta_dict
from hsextract.reftimeseries.utils import extract_referenced_timeseries_metadata
from hsextract.timeseries.utils import extract_metadata as extract_timeseries_metadata, extract_metadata_csv
from hsextract.file_utils import file_metadata

from pathlib import PurePath


def _to_metadata_path(filepath: str):
    filepath = filepath + ".json"
    return os.path.join(".hs", filepath)

def extract_metadata_with_file_path(type: str, filepath):
    extracted_metadata = extract_metadata(type, filepath)
    if extracted_metadata:
        metadata_path = _to_metadata_path(filepath)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
        with open(metadata_path, "w") as f:
            f.write(json.dumps(extracted_metadata, indent=2))
    return extracted_metadata

def extract_metadata(type: str, filepath):
    try:
        _, extracted_metadata = _extract_metadata(type, filepath)
    except Exception as e:
        return None
    if extracted_metadata:
        all_file_metadata = []
        for f in extracted_metadata["files"]:
            all_file_metadata.append(file_metadata(f))
        extracted_metadata["files"] = all_file_metadata
    return extracted_metadata

def _extract_metadata(type: str, filepath):
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
    current_directory = os.getcwd()
    try:
        os.chdir(path)
        sorted_files, categorized_files = prepare_files()

        netcdf_files = categorized_files["netcdf"]
        del categorized_files["netcdf"]
        tasks = []
        for category, files in categorized_files.items():
            for file in files:
                tasks.append(asyncio.get_running_loop().run_in_executor(None, extract_metadata_with_file_path, category, file))

        # The netcdf library does not seem to be thread safe, running them in this thread
        for file in netcdf_files:
            extract_metadata_with_file_path("netcdf", file)

        if tasks:
            await asyncio.gather(*tasks)

        file_tasks = []
        for file in sorted_files:
            file_tasks.append(asyncio.get_running_loop().run_in_executor(None, file_metadata, file))

        if file_tasks:
            metadata_path = os.path.join(".hs", "file_manifest.json")
            os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
            files_with_metadata = await asyncio.gather(*file_tasks)
            with open(metadata_path, "w") as f:
                f.write(json.dumps(files_with_metadata, indent=2))
    finally:
        os.chdir(current_directory)
