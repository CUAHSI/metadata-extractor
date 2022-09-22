import pytest
import json

from hsextract.feature.utils import extract_metadata_and_files


def test_watershed_extraction():
    metadata_dict = extract_metadata_and_files("tests/test_files/watersheds/watersheds.shp")
    metadata_json = json.loads(json.dumps(metadata_dict))

    with open("tests/outputs/feature.json", "r") as f:
        expected_str = f.read()
        expected_json = json.loads(expected_str)
    
    assert metadata_json == expected_json
