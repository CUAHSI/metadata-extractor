import asyncio
import json
import logging
import os
from pathlib import Path

from hsextract.adapters.hydroshare import HydroshareMetadataAdapter
from hsextract.feature.utils import extract_metadata_and_files
from hsextract.file_utils import file_metadata
from hsextract.listing.utils import prepare_files
from hsextract.models.core import CoreMetadata
from hsextract.models.dataset import AdditionalType, ScientificDataset
from hsextract.netcdf.utils import get_nc_meta_dict
from hsextract.raster.utils import extract_from_tif_file
from hsextract.reftimeseries.utils import extract_referenced_timeseries_metadata
from hsextract.timeseries.utils import extract_metadata as extract_timeseries_metadata
from hsextract.timeseries.utils import extract_metadata_csv


def _to_metadata_path(type: str, filepath: str, output_path: str):
    if type != "user_meta":
        return os.path.join(output_path, filepath + ".json")
    dirname, _ = os.path.split(filepath)
    return os.path.join(output_path, dirname, "dataset_metadata.json")


def extract_metadata_with_file_path(
    type: str,
    input_path: str,
    user_metadata_filename: str,
    output_path: str,
    output_base_url: str,
):
    extracted_metadata = extract_metadata(
        type, input_path, output_base_url, user_metadata_filename
    )

    if extracted_metadata:
        input_path = _to_metadata_path(type, input_path, output_path)
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        with open(input_path, "w") as f:
            f.write(json.dumps(extracted_metadata, indent=2))
    return input_path, extracted_metadata is not None


def extract_metadata(
    type: str, input_path: str, output_base_url: str, user_metadata_filename: str
):

    # extract metadata - consider the specific type of file, e.g. netcdf, geotiff, etc.
    try:
        extracted_metadata = _extract_metadata(type, input_path)
    except Exception as e:
        logging.exception(f"Failed to extract {type} metadata from {input_path}.")
        return None

    if os.path.basename(input_path) == user_metadata_filename:
        path = os.path.dirname(input_path)
        extracted_metadata["url"] = os.path.join(
            output_base_url, path, "dataset_metadata.json"
        )
    else:
        extracted_metadata["url"] = os.path.join(output_base_url, input_path)

    # combine all extracted filemetadata into a list
    adapter = HydroshareMetadataAdapter()
    all_file_metadata = []
    for f in extracted_metadata["content_files"]:
        f_md, _ = file_metadata(f)
        all_file_metadata.append(f_md)
    del extracted_metadata["content_files"]

    # convert metadata into SchemaOrg representations
    if type == "user_meta":
        print(f"Creating Core metadata record for {type}...", end="")
        extracted_metadata["associatedMedia"] = all_file_metadata
        # updating to core metadata schema
        catalog_record = json.loads(CoreMetadata.construct(**extracted_metadata).json())
        print("done")
        return catalog_record
    else:
        print(f"Creating Scientific metadata record for {type}...", end="")
        extracted_metadata["associatedMedia"] = all_file_metadata

        #        catalog_record = json.loads(
        #            adapter.to_catalog_record(extracted_metadata).json()
        #        )
        # TODO: Start Here
        scientific_dataset = adapter.to_scientific_dataset_record(
            extracted_metadata
        ).model_dump(mode="json", exclude_none=True)

        # check for user metadata attached content types
        user_meta_content_type_path = input_path + "." + user_metadata_filename
        if os.path.exists(user_meta_content_type_path):
            with open(user_meta_content_type_path, "r") as f:
                user_metadata = json.loads(f.read())
            # catalog_record.update(user_metadata)
            scientific_dataset.update(user_metadata)

        print("done")
        # return catalog_record
        return scientific_dataset


def _extract_metadata(type: str, filepath):
    extension = os.path.splitext(filepath)[1]
    metadata = None
    if type == "raster":
        metadata = extract_from_tif_file(filepath)
        # metadata["type"] = "GeographicRasterAggregation"
        metadata["type"] = AdditionalType.GEOGRAPHIC_RASTER.value
    elif type == "feature":
        metadata = extract_metadata_and_files(filepath)
        # metadata["type"] = "GeographicFeatureAggregation"
        metadata["type"] = AdditionalType.GEOGRAPHIC_FEATURE.value
    elif type == "netcdf":
        metadata = get_nc_meta_dict(filepath)
        # metadata["type"] = "MultidimensionalAggregation"
        metadata["type"] = AdditionalType.MULTIDIMENSIONAL.value
    elif type == "timeseries":
        if extension == ".csv":
            metadata = extract_metadata_csv(filepath)
        elif extension == ".sqlite":
            metadata = extract_timeseries_metadata(filepath)
        # metadata["type"] = "TimeSeriesAggregation"
        # TODO: TABULAR is a more generic type, but we can use it for now however it may
        # not be appeopriate for all time series datasets, e.g. sqlite.
        metadata["type"] = AdditionalType.TABULAR.value
    elif type == "reftimeseries":
        metadata = extract_referenced_timeseries_metadata(filepath)
        metadata["type"] = "ReferencedTimeSeriesAggregation"
    elif type == "user_meta":
        metadata = {}
        if os.path.exists(filepath):
            with open(filepath) as f:
                metadata = json.loads(f.read())
        metadata_file_dir, filename = os.path.split(filepath)
        metadata["content_files"] = [
            str(f)
            for f in Path(f"./{metadata_file_dir}").rglob("*")
            if not str(f).endswith(filename) and os.path.isfile(str(f))
        ]
        if "type" not in metadata:
            # Check type to ensure ResourceType isn't overwritten if provided
            metadata["type"] = "FileSetAggregation"

    return metadata


def read_metadata(path: str):
    with open(path, "r") as f:
        return json.loads(f.read())


async def list_and_extract(
    input_path: str,
    output_path: str,
    input_base_url: str,
    output_base_url: str,
    user_metadata_filename: str,
):
    current_directory = os.getcwd()
    try:
        os.chdir(input_path)
        sorted_files, categorized_files = prepare_files(user_metadata_filename)
        netcdf_files = categorized_files["netcdf"]
        del categorized_files["netcdf"]
        tasks = []

        if (
            "user_meta" not in categorized_files
            or user_metadata_filename not in categorized_files["user_meta"]
        ):
            categorized_files["user_meta"].append(user_metadata_filename)

        for category, files in categorized_files.items():
            for file in files:
                tasks.append(
                    asyncio.get_running_loop().run_in_executor(
                        None,
                        extract_metadata_with_file_path,
                        category,
                        file,
                        user_metadata_filename,
                        output_path,
                        output_base_url,
                    )
                )

        for file in sorted_files:
            tasks.append(
                asyncio.get_running_loop().run_in_executor(None, file_metadata, file)
            )

        results = []
        if tasks:
            results.extend(await asyncio.gather(*tasks))

        # The netcdf library does not seem to be thread safe, running them in this thread
        for file in netcdf_files:
            results.append(
                extract_metadata_with_file_path(
                    "netcdf", file, user_metadata_filename, output_path, output_base_url
                )
            )

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

            has_part = []
            for has_part_file in has_part_files:
                metadata_json = read_metadata(has_part_file)
                name = metadata_json.get(
                    "name", None
                )  # get the name if it exists, if it doesn't exist set default as warning.
                if not name:
                    name = "Not Found and name is required"
                has_part_file = os.path.relpath(has_part_file, output_path)
                has_part.append(
                    {
                        "@type": "CreativeWork",
                        "name": name,
                        "description": (
                            metadata_json["description"]
                            if "description" in metadata_json
                            else None
                        ),
                        "url": os.path.join(output_base_url, has_part_file),
                    }
                )
            for has_part_file in has_part_files:
                metadata_json = read_metadata(has_part_file)
                with open(has_part_file, "w") as f:
                    is_part_of_file = os.path.relpath(
                        dataset_metadata_file, output_path
                    )
                    metadata_json["isPartOf"] = [
                        os.path.join(output_base_url, is_part_of_file)
                    ]
                    f.write(json.dumps(metadata_json, indent=2))
            with open(dataset_metadata_file, "r") as f:
                metadata_json = json.loads(f.read())

            metadata_json["hasPart"] = has_part

            if "associatedMedia" in metadata_json:
                associated_media = []
                for md in metadata_json["associatedMedia"]:
                    md["contentUrl"] = os.path.join(input_base_url, md["contentUrl"])
                    associated_media.append(md)
                metadata_json["associatedMedia"] = associated_media

            with open(dataset_metadata_file, "w") as f:
                f.write(json.dumps(metadata_json, indent=2))

        # add base_url to the contentUrl of the associatedMedia for the metadata_manifest files
        for meta_manifest_item in metadata_manifest:
            meta_manifest_file = list(meta_manifest_item.keys())[0]
            with open(meta_manifest_file, "r") as f:
                metadata = json.loads(f.read())
                if "associatedMedia" in metadata:
                    associated_media = []
                    for md in metadata["associatedMedia"]:
                        if not md["contentUrl"].startswith(input_base_url):
                            md["contentUrl"] = os.path.join(
                                input_base_url, md["contentUrl"]
                            )
                        associated_media.append(md)
                    metadata["associatedMedia"] = associated_media
                if "url" in metadata:
                    metadata["url"] = os.path.join(
                        output_base_url,
                        os.path.relpath(meta_manifest_file, output_path),
                    )
            with open(meta_manifest_file, "w") as f:
                f.write(json.dumps(metadata, indent=2))

    finally:
        os.chdir(current_directory)
