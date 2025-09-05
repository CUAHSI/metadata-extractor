import json
import os
import boto3

from hsextract_dbos.app.main import workflow_metadata_extraction
import pytest

s3_config = {
    "endpoint_url": os.environ.get("AWS_S3_ENDPOINT", "https://s3.beta.hydroshare.org"),
    "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "YOUR_ACCESS_KEY"),
    "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "YOUR_SECRET_KEY")
}
s3_client = boto3.client('s3', **s3_config)


def _assert_from_file(filename, metadata_json):
    with open(filename) as f:
        expected_str = f.read()
        expected_json = json.loads(expected_str)

    assert metadata_json == expected_json


def _assert_raster_from_file(filename, metadata_json):
    with open(filename) as f:
        expected_str = f.read()
        expected_json = json.loads(expected_str)

    # remove the minimumValue, maximumValue and projection_string because gdal is inconsistent
    del metadata_json['band_information']['maximum_value']
    del expected_json['band_information']['maximum_value']

    del metadata_json['band_information']['minimum_value']
    del expected_json['band_information']['minimum_value']

    del metadata_json['spatial_reference']['projection_string']
    del expected_json['spatial_reference']['projection_string']

    assert metadata_json == expected_json

'''
def test_rasters_extraction(test_file_dir):
    metadata_dict = workflow_metadata_extraction("raster", "rasters/logan.vrt", "https://www.hydroshare.org/resource/s3")

    _assert_raster_from_file("../outputs/raster.json", metadata_dict)


def test_raster_single_extraction(test_file_dir):
    all_metadata_json = workflow_metadata_extraction(
        "raster", "rasters/single/logan1.tif", "https://www.hydroshare.org/resource/s3"
    )

    _assert_from_file("../outputs/raster-single.json", all_metadata_json)


def test_raster_single_extraction(test_file_dir):
    all_metadata_json = workflow_metadata_extraction(
        "raster", "rasters/single/logan1.tif", "https://www.hydroshare.org/resource/s3"
    )

    _assert_raster_from_file("../outputs/raster-single.json", all_metadata_json)


def test_features_watersheds_extraction(test_file_dir):
    metadata_dict = workflow_metadata_extraction("feature", "watersheds/watersheds.shp", "https://www.hydroshare.org/resource/s3")

    _assert_from_file("../outputs/feature.json", metadata_dict)


def test_reftimeseries_extraction(test_file_dir):
    ref_timeseries_json = workflow_metadata_extraction(
        "reftimeseries",
        "reftimeseries/multi_sites_formatted_version1.0.refts.json",
        "https://www.hydroshare.org/resource/s3",
    )

    _assert_from_file("../outputs/reftimeseries.json", ref_timeseries_json)


def test_timeseries_sqlite_extraction(test_file_dir):
    timeseries_json = workflow_metadata_extraction(
        "timeseries", "timeseries/ODM2_Multi_Site_One_Variable.sqlite", "https://www.hydroshare.org/resource/s3"
    )

    _assert_from_file("../outputs/timeseries.json", timeseries_json)


def test_timeseries_csv_extraction(test_file_dir):
    timeseries_json = workflow_metadata_extraction(
        "timeseries", "timeseries/ODM2_Multi_Site_One_Variable_Test.csv", "https://www.hydroshare.org/resource/s3"
    )

    _assert_from_file("../outputs/timeseries-csv.json", timeseries_json)
'''

def test_netcdf_extraction():
    # TODO: push to fixture that resets the environment
    delete_s3_metadata_json("sblack/md/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json")
    delete_s3_metadata_json("sblack/md/21a44ea2b87e4f0c930c9eefb1078b00/netcdf/netcdf_valid.nc")
    delete_s3_metadata_json("sblack/.md/21a44ea2b87e4f0c930c9eefb1078b00/netcdf/netcdf_valid.nc")

    # Stage system metadata to test
    write_s3_metadata_json("sblack/.md/21a44ea2b87e4f0c930c9eefb1078b00/system_metadata.json", {"system_metadata": "this is system metadata"})

    # send a file updated event for a valid netcdf file
    workflow_metadata_extraction("sblack/21a44ea2b87e4f0c930c9eefb1078b00/data/contents/netcdf/netcdf_valid.nc")
    # read in the resulting resource metadata file
    result_resource_metadata = read_s3_metadata_json("sblack/md/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json")

    #write_metadata_to_file(f"test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json", result_resource_metadata)
    expected_resource_metadata = read_metadata_json("test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json")
    assert result_resource_metadata == expected_resource_metadata

    # read in the resulting netcdf metadata file
    result_netcdf_metadata = read_s3_metadata_json("sblack/md/21a44ea2b87e4f0c930c9eefb1078b00/netcdf/netcdf_valid.nc.json")
    #write_metadata_to_file(f"test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/netcdf/netcdf_valid.nc.json", result_netcdf_metadata)
    expected_netcdf_metadata = read_metadata_json("test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/netcdf/netcdf_valid.nc.json")
    assert result_netcdf_metadata == expected_netcdf_metadata

    #_assert_from_file("../outputs/netcdf.json", all_metadata_json)


def test_resource_extraction():
    delete_s3_metadata_json("sblack/md/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json")
    # Stage system metadata to test
    write_s3_metadata_json("sblack/.md/21a44ea2b87e4f0c930c9eefb1078b00/system_metadata.json", {"system_metadata": "this is system metadata"})

    workflow_metadata_extraction("sblack/21a44ea2b87e4f0c930c9eefb1078b00/data/contents/hs_user_meta.json")

    # read in the resulting resource metadata file
    result_resource_metadata = read_s3_metadata_json("sblack/md/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json")

    #write_metadata_to_file(f"test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json", result_resource_metadata)
    expected_resource_metadata = read_metadata_json("test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json")
    assert result_resource_metadata == expected_resource_metadata
    

'''
def test_feature_states_extraction(test_file_dir):
    all_metadata_json = workflow_metadata_extraction("sblack/21a44ea2b87e4f0c930c9eefb1078b00/data/contents/states/states.shp")

    _assert_from_file("../outputs/feature-states.json", all_metadata_json)
'''

def delete_s3_metadata_json(path: str):
    bucket, key = path.split("/", 1)
    s3_client.delete_object(
        Bucket=bucket,
        Key=key
    )

def read_s3_metadata_json(path: str):
    bucket, key = path.split("/", 1)
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )
    file_content = response['Body'].read().decode('utf-8')
    metadata_json = json.loads(file_content)
    return metadata_json

def write_s3_metadata_json(path: str, metadata_json: dict):
    bucket, key = path.split("/", 1)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(metadata_json, indent=2).encode('utf-8'),
        ContentType='application/json'
    )

def read_metadata_json(path: str):
    with open(path) as f:
        return json.loads(f.read())

def write_metadata_to_file(path: str, metadata_json: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(metadata_json, f, indent=2)

'''
def test_threaded_metadata_extraction(cleanup_metadata):
    workflow_metadata_extraction()

    assert os.path.exists("tests/test_files/.hs")
    assert os.path.exists("tests/test_files/.hs/file_manifest.json")
    assert os.path.exists("tests/test_files/.hs/metadata_manifest.json")

    metadata_path = "tests/test_files/.hs/timeseries/ODM2_Multi_Site_One_Variable_Test.csv.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("tests/outputs/timeseries-csv.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/timeseries/ODM2_Multi_Site_One_Variable.sqlite.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("tests/outputs/timeseries.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/reftimeseries/multi_sites_formatted_version1.0.refts.json.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("tests/outputs/reftimeseries.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/watersheds/watersheds.shp.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("tests/outputs/feature.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/states/states.shp.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("tests/outputs/feature-states.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/rasters/logan.vrt.json"
    assert os.path.exists(metadata_path)
    _assert_raster_from_file("tests/outputs/raster.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/rasters/single/logan1.tif.json"
    assert os.path.exists(metadata_path)
    _assert_raster_from_file("tests/outputs/raster-single.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/netcdf/netcdf_valid.nc.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("tests/outputs/netcdf.json", read_metadata_json(metadata_path))
'''