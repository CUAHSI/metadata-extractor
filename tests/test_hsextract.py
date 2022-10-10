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
    del metadata_json['_band_information_0']['maximumValue']
    del expected_json['_band_information_0']['maximumValue']

    del metadata_json['_band_information_0']['minimumValue']
    del expected_json['_band_information_0']['minimumValue']

    del metadata_json['_ori_coverage']['_value']['projection_string']
    del expected_json['_ori_coverage']['_value']['projection_string']

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

def test_feature_states_extraction():
    all_metadata_json = extract_metadata("feature", "tests/test_files/states/states.shp")

    _assert_from_file("feature-states.json", all_metadata_json)


@pytest.mark.asyncio
async def test_threaded_metadata_extraction():

    sorted_files, extracted_metadata = await list_and_extract("tests/test_files")

    unextracted_count = 0
    for file_path, metadata in extracted_metadata:
        if file_path.endswith("netcdf_valid.nc"):
            _assert_from_file("netcdf.json", metadata)
        elif file_path.endswith("ODM2_Multi_Site_One_Variable_Test.csv"):
            _assert_from_file("timeseries-csv.json", metadata)
        elif file_path.endswith("ODM2_Multi_Site_One_Variable.sqlite"):
            _assert_from_file("timeseries.json", metadata)
        elif file_path.endswith("multi_sites_formatted_version1.0.refts.json"):
            _assert_from_file("reftimeseries.json", metadata)
        elif file_path.endswith("watersheds.shp"):
            _assert_from_file("feature.json", metadata)
        elif file_path.endswith("states.shp"):
            _assert_from_file("feature-states.json", metadata)
        elif file_path.endswith("logan.vrt"):
            _assert_raster_from_file("raster.json", metadata)
        elif file_path.endswith("single/logan1.tif"):
            _assert_raster_from_file("raster-single.json", metadata)
        elif file_path.endswith("hs_user_meta.json"):
            assert metadata == None
        else:
            assert metadata == None
            unextracted_count = unextracted_count + 1
    assert unextracted_count == 2
    assert len(extracted_metadata) == 11
    assert len(sorted_files) == 26
