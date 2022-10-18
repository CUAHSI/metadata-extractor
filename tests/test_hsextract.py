import asyncio
import shutil
import pytest
import json
import os

from hsextract.utils import list_and_extract, extract_metadata


pytest_plugins = ('pytest_asyncio',)

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

@pytest.fixture()
def test_file_dir():
    current_dir = os.getcwd()
    os.chdir("tests/test_files")
    yield
    os.chdir(current_dir)

def test_rasters_extraction(test_file_dir):
    metadata_dict = extract_metadata("raster", "rasters/logan.vrt")

    _assert_raster_from_file("../outputs/raster.json", metadata_dict)

def test_raster_single_extraction(test_file_dir):
    all_metadata_json = extract_metadata("raster", "rasters/single/logan1.tif")

    _assert_from_file("../outputs/raster-single.json", all_metadata_json)

def test_raster_single_extraction(test_file_dir):
    all_metadata_json = extract_metadata("raster", "rasters/single/logan1.tif")

    _assert_raster_from_file("../outputs/raster-single.json", all_metadata_json)

def test_features_watersheds_extraction(test_file_dir):
    metadata_dict = extract_metadata("feature", "watersheds/watersheds.shp")

    _assert_from_file("../outputs/feature.json", metadata_dict)

def test_reftimeseries_extraction(test_file_dir):
    ref_timeseries_json = extract_metadata("reftimeseries", "reftimeseries/multi_sites_formatted_version1.0.refts.json")

    _assert_from_file("../outputs/reftimeseries.json", ref_timeseries_json)

def test_timeseries_sqlite_extraction(test_file_dir):
    timeseries_json = extract_metadata("timeseries", "timeseries/ODM2_Multi_Site_One_Variable.sqlite")

    _assert_from_file("../outputs/timeseries.json", timeseries_json)

def test_timeseries_csv_extraction(test_file_dir):
    timeseries_json = extract_metadata("timeseries", "timeseries/ODM2_Multi_Site_One_Variable_Test.csv")

    _assert_from_file("../outputs/timeseries-csv.json", timeseries_json)

def test_netcdf_extraction(test_file_dir):
    all_metadata_json = extract_metadata("netcdf", "netcdf/netcdf_valid.nc")

    _assert_from_file("../outputs/netcdf.json", all_metadata_json)

def test_feature_states_extraction(test_file_dir):
    all_metadata_json = extract_metadata("feature", "states/states.shp")

    _assert_from_file("../outputs/feature-states.json", all_metadata_json)

def read_metadata_json(path: str):
    with open(path) as f:
        return json.loads(f.read())

@pytest.fixture()
def cleanup_metadata():
    yield
    if os.path.exists("tests/test_files/.hs"):
        shutil.rmtree("tests/test_files/.hs")


@pytest.mark.asyncio
async def test_threaded_metadata_extraction(cleanup_metadata):

    await list_and_extract("tests/test_files")

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
