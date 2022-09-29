import pytest
import json

from hsextract.feature.utils import extract_metadata_and_files
from hsextract.raster.utils import extract_from_tif_file


def test_features_watersheds_extraction():
    metadata_dict = extract_metadata_and_files("tests/test_files/watersheds/watersheds.shp")
    metadata_json = json.loads(json.dumps(metadata_dict))

    with open("tests/outputs/feature.json", "r") as f:
        expected_str = f.read()
        expected_json = json.loads(expected_str)
    
    assert metadata_json == expected_json

def test_rasters_extraction():
    metadata_dict = extract_from_tif_file("tests/test_files/rasters/logan.vrt")
    metadata_json = json.loads(json.dumps(metadata_dict))
    

    with open("tests/outputs/raster.json", "r") as f:
        expected_str = f.read()
        expected_json = json.loads(expected_str)
    
    # remove the maximumValue because gdal is inconsistent
    del metadata_json['_band_information_0']['maximumValue']
    del expected_json['_band_information_0']['maximumValue']
    
    assert metadata_json == expected_json
