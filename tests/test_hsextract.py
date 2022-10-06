import asyncio
import pytest
import json
import os

from hsextract.feature.utils import extract_metadata_and_files
from hsextract.raster.utils import extract_from_tif_file
from hsextract.netcdf.utils import get_nc_meta_dict
from hsextract.reftimeseries.utils import extract_referenced_timeseries_metadata
from hsextract.timeseries.utils import extract_metadata as extract_timeseries_metadata, extract_metadata_csv
from hsextract.utils import list_and_extract


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
    metadata_dict = extract_from_tif_file("tests/test_files/rasters/logan.vrt")

    _assert_raster_from_file("raster.json", metadata_dict)

def test_features_watersheds_extraction():
    metadata_dict = extract_metadata_and_files("tests/test_files/watersheds/watersheds.shp")

    _assert_from_file("feature.json", metadata_dict)

def test_reftimeseries_extraction():
    ref_timeseries_json = extract_referenced_timeseries_metadata("tests/test_files/reftimeseries/multi_sites_formatted_version1.0.refts.json")

    _assert_from_file("reftimeseries.json", ref_timeseries_json)

def test_timeseries_sqlite_extraction():
    timeseries_json = extract_timeseries_metadata("tests/test_files/timeseries/ODM2_Multi_Site_One_Variable.sqlite")

    _assert_from_file("timeseries.json", timeseries_json)

def test_timeseries_csv_extraction():
    timeseries_json = extract_metadata_csv("tests/test_files/timeseries/ODM2_Multi_Site_One_Variable_Test.csv")

    _assert_from_file("timeseries-csv.json", timeseries_json)

def test_netcdf_extraction():
    all_metadata_json = get_nc_meta_dict("tests/test_files/netcdf/netcdf_valid.nc")

    _assert_from_file("netcdf.json", all_metadata_json)


@pytest.mark.asyncio
async def test_threaded_metadata_extraction():

    sorted_files, extracted_metadata = await list_and_extract("tests/test_files")

    unextracted_count = 0
    for file_path, metadata in extracted_metadata:
        if file_path.endswith("netcdf_valid.nc"):
            _assert_from_file("netcdf.json", metadata)
        elif file_path.endswith("netcdf_invalid.nc"):
            pass
        elif file_path.endswith("ODM2_Multi_Site_One_Variable_Test.csv"):
            _assert_from_file("timeseries-csv.json", metadata)
        elif file_path.endswith("ODM2_Multi_Site_One_Variable.sqlite"):
            _assert_from_file("timeseries.json", metadata)
        elif file_path.endswith("multi_sites_formatted_version1.0.refts.json"):
            _assert_from_file("reftimeseries.json", metadata)
        elif file_path.endswith("watersheds.shp"):
            _assert_from_file("feature.json", metadata)
        elif file_path.endswith("logan.vrt"):
            _assert_raster_from_file("raster.json", metadata)
        elif file_path.endswith("hs_user_meta.json"):
            assert metadata == None
        else:
            assert metadata == None
            unextracted_count = unextracted_count + 1
    assert unextracted_count == 0
    assert len(extracted_metadata) == 8
    assert len(sorted_files) == 18
