import json
import os
import boto3

from hsextract_dbos.app.main import ContentType, workflow_metadata_extraction
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
    ,
    (
        "sblack/21a44ea2b87e4f0c930c9eefb1078b00/data/contents/.hs",
        "sblack/21a44ea2b87e4f0c930c9eefb1078b00/data/contents/.hsjsonld",
        "sblack/21a44ea2b87e4f0c930c9eefb1078b00/data/contents",
        "netcdf/netcdf_valid.nc",
        "test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/netcdf/netcdf_valid.nc.json",
        "test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json"
    )
'''
def test_metadataobject():
    from hsextract_dbos.app.main import MetadataObject
    md = MetadataObject("test-bucket/resourceid/data/contents/hs_user_meta.json", True)
    assert md.file_object_path == "test-bucket/resourceid/data/contents/hs_user_meta.json"
    assert md.file_updated == True
    assert md.resource_contents_path == "test-bucket/resourceid/data/contents"
    assert md.resource_md_path == "test-bucket/resourceid/.hs"
    assert md.resource_md_jsonld_path == "test-bucket/resourceid/.hsjsonld"
    assert md.content_type_md_jsonld_path == None
    assert md.content_type == ContentType.UNKNOWN
    assert md.system_metadata_path == "test-bucket/resourceid/.hs/system_metadata.json"
    assert md.user_metadata_path == "test-bucket/resourceid/data/contents/hs_user_meta.json"
    assert md.resource_metadata_path == "test-bucket/resourceid/.hsjsonld/dataset_metadata.json"


@pytest.fixture
def s3_resource_setup_teardown():
    # Setup: Create a temporary bucket for testing
    test_bucket = "test-bucket"
    try:
        s3_client.create_bucket(Bucket=test_bucket)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    # Upload all local files in ../tests/test_file_output to the resource
    test_file_output_dir = "test_files/"
    for root, _, files in os.walk(test_file_output_dir):
        for file in files:
            file_path = os.path.join(root, file)
            s3_key = os.path.join("resource_id", "data", "contents", file)
            with open(file_path, "rb") as f:
                s3_client.upload_fileobj(f, test_bucket, s3_key)

    yield test_bucket  # Provide the bucket name to the test


def test_resource_extraction():
    delete_s3_metadata_json("test-bucket/resource_id/.hsjsonld/dataset_metadata.json")
    # Stage system metadata to test
    write_s3_metadata_json("test-bucket/resource_id/.hs/system_metadata.json", {"system_metadata": "this is system metadata"})

    workflow_metadata_extraction("test-bucket/resource_id/data/contents/hs_user_meta.json")

    # read in the resulting resource metadata file
    result_resource_metadata = read_s3_metadata_json("test-bucket/resource_id/.hsjsonld/dataset_metadata.json")

    #write_metadata_to_file(f"test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json", result_resource_metadata)
    #expected_resource_metadata = read_metadata_json("test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json")
    assert result_resource_metadata


'''
    # Teardown: Delete all objects and the bucket
    response = s3_client.list_objects_v2(Bucket=test_bucket)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3_client.delete_object(Bucket=test_bucket, Key=obj['Key'])
    s3_client.delete_bucket(Bucket=test_bucket)

@pytest.mark.parametrize("metadata_path, jsonld_metadata_path, contents_path, relative_file_path, expected_metadata_path, expected_resource_metadata_path", [
    (
        "test-bucket/21a44ea2b87e4f0c930c9eefb1078b00/data/contents/.hs",
        "test-bucket/21a44ea2b87e4f0c930c9eefb1078b00/data/contents/.hs/jsonld",
        "test-bucket/21a44ea2b87e4f0c930c9eefb1078b00/data/contents",
        "rasters/logan1.vrt",
        "test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/rasters/logan1.vrt.json",
        "test_files_output/21a44ea2b87e4f0c930c9eefb1078b00/dataset_metadata.json"
    )
])
def test_content_type_extraction(s3_resource_setup_teardown, metadata_path, jsonld_metadata_path, contents_path, relative_file_path, expected_metadata_path, expected_resource_metadata_path):
    # Delete existing metadata
    resource_jsonld_path = f"{jsonld_metadata_path}/dataset_metadata.json"
    resource_metadata_path = f"{metadata_path}/dataset_metadata.json"
    #delete_s3_metadata_json(resource_jsonld_path)
    #delete_s3_metadata_json(resource_metadata_path)
    #delete_s3_metadata_json(f"{metadata_path}/{relative_file_path}.json")

    # Stage system metadata to test
    write_s3_metadata_json(f"{metadata_path}/system_metadata.json", {"system_metadata": "this is system metadata"})

    # Trigger metadata extraction
    workflow_metadata_extraction(f"{contents_path}/{relative_file_path}")

    # Validate resource metadata
    result_resource_metadata = read_s3_metadata_json(resource_jsonld_path)
    expected_resource_metadata = read_metadata_json(expected_resource_metadata_path)
    assert result_resource_metadata == expected_resource_metadata

    # Validate content type metadata
    result_metadata = read_s3_metadata_json(f"{metadata_path}/{relative_file_path}.json")
    expected_metadata = read_metadata_json(expected_metadata_path)
    assert result_metadata == expected_metadata
'''
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