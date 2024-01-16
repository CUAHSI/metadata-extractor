import asyncio
import os
import json
import logging

from pathlib import Path

from hsextract.adapters.hydroshare import HydroshareMetadataAdapter
from hsextract.listing.utils import prepare_files
from hsextract.raster.utils import extract_from_tif_file
from hsextract.feature.utils import extract_metadata_and_files
from hsextract.netcdf.utils import get_nc_meta_dict
from hsextract.reftimeseries.utils import extract_referenced_timeseries_metadata
from hsextract.timeseries.utils import extract_metadata as extract_timeseries_metadata, extract_metadata_csv
from hsextract.file_utils import file_metadata


def _to_metadata_path(filepath: str, user_metadata_filename: str):
    if not filepath.endswith(user_metadata_filename):
        filepath = filepath + ".json"
    return os.path.join(".hs", filepath)


def extract_metadata_with_file_path(type: str, filepath: str, user_metadata_filename: str):
    extracted_metadata = extract_metadata(type, filepath)
    if extracted_metadata:
        metadata_path = _to_metadata_path(filepath, user_metadata_filename)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
        with open(metadata_path, "w") as f:
            f.write(json.dumps(extracted_metadata, indent=2))
    return filepath, extracted_metadata is not None


def extract_metadata(type: str, filepath):
    try:
        _, extracted_metadata = _extract_metadata(type, filepath)
    except Exception as e:
        logging.exception(f"Failed to extract {type} metadata from {filepath}.")
        return None
    adapter = HydroshareMetadataAdapter()
    all_file_metadata = []
    for f in extracted_metadata["content_files"]:
        f_md, _ = file_metadata(f)
        all_file_metadata.append(f_md)
    extracted_metadata["content_files"] = all_file_metadata
    catalog_record = json.loads(adapter.to_catalog_record(extracted_metadata).json())
    return catalog_record


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
    elif type == "user_meta":
        with open(filepath) as f:
            metadata = json.loads(f.read())
        metadata_file_dir, filename = os.path.split(filepath)
        metadata["content_files"] = [
            str(f)
            for f in Path(f'./{metadata_file_dir}').rglob('*')
            if not str(f).endswith(filename) and os.path.isfile(str(f))
        ]

    return filepath, metadata


async def list_and_extract(path: str, user_metadata_filename: str):
    current_directory = os.getcwd()
    try:
        os.chdir(path)
        sorted_files, categorized_files = prepare_files(user_metadata_filename)

        netcdf_files = categorized_files["netcdf"]
        del categorized_files["netcdf"]
        tasks = []
        for category, files in categorized_files.items():
            for file in files:
                tasks.append(
                    asyncio.get_running_loop().run_in_executor(
                        None, extract_metadata_with_file_path, category, file, user_metadata_filename
                    )
                )

        for file in sorted_files:
            tasks.append(asyncio.get_running_loop().run_in_executor(None, file_metadata, file))

        results = []
        if tasks:
            results.extend(await asyncio.gather(*tasks))

        # The netcdf library does not seem to be thread safe, running them in this thread
        for file in netcdf_files:
            results.append(extract_metadata_with_file_path("netcdf", file, user_metadata_filename))

        metadata_manifest = [
            {file_path: f"{file_path}.json"}
            for file_path, extracted in results
            if extracted == True and not file_path.endswith(user_metadata_filename)
        ]
        dataset_metadata_files = [
            file_path
            for file_path, extracted in results
            if extracted == True and file_path.endswith(user_metadata_filename)
        ]

        all_files_with_metadata = [file_metadata for file_metadata, extracted in results if extracted is None]

        # write an empty json file if one is not provided
        if user_metadata_filename not in dataset_metadata_files:
            dataset_metadata_files.append(user_metadata_filename)
            with open(f".hs/{user_metadata_filename}", "w+") as f:
                f.write("{}")

        for dataset_metadata_file in dataset_metadata_files:
            dirname, _ = os.path.split(dataset_metadata_file)
            has_part_files = [
                list(metadata.keys())[0]
                for metadata in metadata_manifest
                if list(metadata.keys())[0].startswith(dirname)
            ]
            has_part = []
            for has_part_file in has_part_files:
                with open(f".hs/{has_part_file}.json", "r") as f:
                    metadata_json = json.loads(f.read())
                    has_part.append(
                        {
                            "@type": "CreativeWork",
                            "name": metadata_json["name"],
                            "description": metadata_json["description"],
                            "url": has_part_file,
                        }
                    )
            if has_part:
                with open(f".hs/{dataset_metadata_file}", "r") as f:
                    metadata_json = json.loads(f.read())

                metadata_json["hasPart"] = has_part
                os.remove(f".hs/{dataset_metadata_file}")

                if dirname:
                    with open(f".hs/{dirname}/dataset_metadata.json", "w") as f:
                        f.write(json.dumps(metadata_json, indent=2))
                else:
                    with open(".hs/dataset_metadata.json", "w") as f:
                        metadata_json["associatedMedia"] = all_files_with_metadata
                        f.write(json.dumps(metadata_json, indent=2))

    finally:
        os.chdir(current_directory)
