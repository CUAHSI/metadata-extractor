import asyncio
import pytest
import json
import os

from hsextract.utils import list_and_extract, extract_metadata


pytest_plugins = ('pytest_asyncio',)

def _assert_from_file(filename, metadata_json):
    with open(os.path.join("tests", "outputs", filename)) as f:
        expected_str = f.read()
        expected_json = json.loads(expected_str)
    
    assert metadata_json == expected_json

def _assert_raster_from_file(filename, metadata_json):
    with open(os.path.join("tests", "outputs", filename)) as f:
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


def test_rasters_extraction():
    metadata_dict = extract_metadata("raster", "tests/test_files/rasters/logan.vrt")

    _assert_raster_from_file("raster.json", metadata_dict)

def test_raster_single_extraction():
    all_metadata_json = extract_metadata("raster", "tests/test_files/rasters/single/logan1.tif")

    _assert_raster_from_file("raster-single.json", all_metadata_json)

def test_features_watersheds_extraction():
    metadata_dict = extract_metadata("feature", "tests/test_files/watersheds/watersheds.shp")

    _assert_from_file("feature.json", metadata_dict)

def test_reftimeseries_extraction():
    ref_timeseries_json = extract_metadata("reftimeseries", "tests/test_files/reftimeseries/multi_sites_formatted_version1.0.refts.json")

    _assert_from_file("reftimeseries.json", ref_timeseries_json)

def test_timeseries_sqlite_extraction():
    timeseries_json = extract_metadata("timeseries", "tests/test_files/timeseries/ODM2_Multi_Site_One_Variable.sqlite")

    _assert_from_file("timeseries.json", timeseries_json)

def test_timeseries_csv_extraction():
    timeseries_json = extract_metadata("timeseries", "tests/test_files/timeseries/ODM2_Multi_Site_One_Variable_Test.csv")

    _assert_from_file("timeseries-csv.json", timeseries_json)

def test_netcdf_extraction():
    all_metadata_json = extract_metadata("netcdf", "tests/test_files/netcdf/netcdf_valid.nc")

    _assert_from_file("netcdf.json", all_metadata_json)
'''
def test_feature_states_extraction():
    all_metadata_json = extract_metadata("feature", "tests/test_files/states/states.shp")

    _assert_from_file("feature-states.json", all_metadata_json)

def read_metadata_json(path: str):
    with open(path) as f:
        return json.loads(f.read())

@pytest.mark.asyncio
async def test_threaded_metadata_extraction():

    await list_and_extract("tests/test_files")

    assert os.path.exists("tests/test_files/.hs")

    metadata_path = "tests/test_files/.hs/netcdf/netcdf_valid.nc.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("netcdf.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/timeseries/ODM2_Multi_Site_One_Variable_Test.csv.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("timeseries-csv.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/timeseries/ODM2_Multi_Site_One_Variable.sqlite.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("timeseries.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/reftimeseries/multi_sites_formatted_version1.0.refts.json.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("reftimeseries.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/watersheds/watersheds.shp.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("feature.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/states/states.shp.json"
    assert os.path.exists(metadata_path)
    _assert_from_file("feature-states.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/rasters/logan.vrt.json"
    assert os.path.exists(metadata_path)
    _assert_raster_from_file("raster.json", read_metadata_json(metadata_path))

    metadata_path = "tests/test_files/.hs/rasters/single/logan1.tif.json"
    assert os.path.exists(metadata_path)
    _assert_raster_from_file("raster-single.json", read_metadata_json(metadata_path))

'''