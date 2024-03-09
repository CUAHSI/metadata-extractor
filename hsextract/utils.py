import asyncio
import os
import json
import logging

from pathlib import Path

from hsextract.adapters.hydroshare import HydroshareMetadataAdapter
from hsextract.listing.utils import prepare_files
from hsextract.models.schema import CoreMetadataDOC
from hsextract.raster.utils import extract_from_tif_file
from hsextract.feature.utils import extract_metadata_and_files
from hsextract.netcdf.utils import get_nc_meta_dict
from hsextract.reftimeseries.utils import extract_referenced_timeseries_metadata
from hsextract.timeseries.utils import extract_metadata as extract_timeseries_metadata, extract_metadata_csv
from hsextract.file_utils import file_metadata


def _to_metadata_path(filepath: str, user_metadata_filename: str):
    if not filepath.endswith(user_metadata_filename):
        return os.path.join(".hs", filepath + ".json")
    dirname, _ = os.path.split(filepath)
    return os.path.join(".hs", dirname, "dataset_metadata.json")


def extract_metadata_with_file_path(type: str, filepath: str, user_metadata_filename: str):
    extracted_metadata = extract_metadata(type, filepath)
    if extracted_metadata:
        filepath = _to_metadata_path(filepath, user_metadata_filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            f.write(json.dumps(extracted_metadata, indent=2))
    return filepath, extracted_metadata is not None


def extract_metadata(type: str, filepath):
    try:
        extracted_metadata = _extract_metadata(type, filepath)
    except Exception as e:
        logging.exception(f"Failed to extract {type} metadata from {filepath}.")
        return None
    adapter = HydroshareMetadataAdapter()
    all_file_metadata = []
    for f in extracted_metadata["content_files"]:
        f_md, _ = file_metadata(f)
        all_file_metadata.append(f_md)
    del extracted_metadata["content_files"]
    if type == "user_meta":
        extracted_metadata["associatedMedia"] = all_file_metadata
        return json.loads(CoreMetadataDOC.construct(**extracted_metadata).json())
    else:
        extracted_metadata["associatedMedia"] = all_file_metadata
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
        metadata = {}
        if os.path.exists(filepath):
            with open(filepath) as f:
                metadata = json.loads(f.read())
        metadata_file_dir, filename = os.path.split(filepath)
        metadata["content_files"] = [
            str(f)
            for f in Path(f'./{metadata_file_dir}').rglob('*')
            if not str(f).endswith(filename) and os.path.isfile(str(f))
        ]

    return metadata


async def list_and_extract(path: str, user_metadata_filename: str, base_url: str):
    current_directory = os.getcwd()
    try:
        os.chdir(path)
        sorted_files, categorized_files = prepare_files(user_metadata_filename)
        netcdf_files = categorized_files["netcdf"]
        del categorized_files["netcdf"]
        tasks = []

        if "user_meta" not in categorized_files or user_metadata_filename not in categorized_files["user_meta"]:
            categorized_files["user_meta"].append(user_metadata_filename)

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
            if extracted and not file_path.endswith("dataset_metadata.json")
        ]
        dataset_metadata_files = [
            file_path
            for file_path, extracted in results
            if extracted and file_path.endswith("dataset_metadata.json")
        ]

        dataset_metadata_files_metadata = {}
        for dataset_metadata_file in dataset_metadata_files:
            with open(dataset_metadata_file, "r") as f:
                has_part_metadata = json.loads(f.read())
            dataset_metadata_files_metadata[dataset_metadata_file] = has_part_metadata

        for dataset_metadata_file in dataset_metadata_files:
            dirname, _ = os.path.split(dataset_metadata_file)
            has_part_files = [
                list(metadata.keys())[0]
                for metadata in metadata_manifest
                if list(metadata.keys())[0].startswith(dirname)
            ]

            has_part = [
                {
                    "@type": "CreativeWork",
                    "name": dataset_metadata_files_metadata[metadata]["name"],
                    "description": dataset_metadata_files_metadata[metadata]["description"],
                    "url": os.path.join(base_url, metadata),
                }
                for metadata in dataset_metadata_files
                if metadata.startswith(dirname) and metadata != dataset_metadata_file
            ]
            for has_part_file in has_part_files:
                with open(has_part_file, "r") as f:
                    metadata_json = json.loads(f.read())
                    has_part.append(
                        {
                            "@type": "CreativeWork",
                            "name": metadata_json["name"] if "name" in metadata_json else None,
                            "description": metadata_json["description"] if "description" in metadata_json else None,
                            "url": os.path.join(base_url, has_part_file),
                        }
                    )
            with open(dataset_metadata_file, "r") as f:
                metadata_json = json.loads(f.read())

            metadata_json["hasPart"] = has_part

            if "associatedMedia" in metadata_json:
                associated_media = []
                has_parts_metadata = []
                for has_part_file in has_part_files:
                    with open(has_part_file, "r") as f:
                        has_parts_metadata.append({has_part_file: json.loads(f.read())})
                for md in metadata_json["associatedMedia"]:
                    content_file_path = md["contentUrl"]
                    # set the name attribute of the associatedMedia to the content file name
                    md["name"] = os.path.basename(content_file_path)
                    md["contentUrl"] = os.path.join(base_url, md["contentUrl"])
                    # add isPartOf to the associatedMedia to link the content file to the metadata file
                    cont_file_dirname, _ = os.path.split(content_file_path)
                    is_parts_of = []
                    for has_part_meta_item in has_parts_metadata:
                        has_part_file = list(has_part_meta_item.keys())[0]
                        meta_file_dirname, _ = os.path.split(has_part_file)
                        # remove the leading ".hs/" from the metadata file dirname for comparison
                        meta_file_dirname = meta_file_dirname[4:]
                        if cont_file_dirname == meta_file_dirname:
                            has_part_metadata = has_part_meta_item[has_part_file]
                            if "associatedMedia" in has_part_metadata:
                                for md_ in has_part_metadata["associatedMedia"]:
                                    if md_["contentUrl"] == content_file_path:
                                        is_part_of = {
                                            "@type": "CreativeWork",
                                            "name": has_part_metadata.get("name", None),
                                            "description": has_part_metadata.get("description", None),
                                            "url": os.path.join(base_url, has_part_file),
                                        }
                                        is_parts_of.append(is_part_of)
                                        break
                    md["isPartOf"] = is_parts_of
                    associated_media.append(md)
                metadata_json["associatedMedia"] = associated_media

            with open(dataset_metadata_file, "w") as f:
                f.write(json.dumps(metadata_json, indent=2))

        # add base_url to the contentUrl of the associatedMedia for the metadata_manifest files
        # and set the name attribute of the associatedMedia to the content file name
        for meta_manifest_item in metadata_manifest:
            meta_manifest_file = list(meta_manifest_item.keys())[0]
            with open(meta_manifest_file, "r") as f:
                metadata = json.loads(f.read())
                if "associatedMedia" in metadata:
                    associated_media = []
                    for md in metadata["associatedMedia"]:
                        # set the name attribute of the associatedMedia to the content file name
                        md["name"] = os.path.basename(md["contentUrl"])
                        if not md["contentUrl"].startswith(base_url):
                            md["contentUrl"] = os.path.join(base_url, md["contentUrl"])
                        associated_media.append(md)
                    metadata["associatedMedia"] = associated_media
            with open(meta_manifest_file, "w") as f:
                f.write(json.dumps(metadata, indent=2))

    finally:
        os.chdir(current_directory)
