import json
from datetime import datetime
import pytest
from hsextract.adapters.hydroshare import HydroshareMetadataAdapter

@pytest.fixture
def resource_metadata():
    with open('data/resource_meta.json', 'r') as f:
        metadata =  json.load(f)
    metadata["sharing_status"] = "published"
    return metadata


def test_to_catalog_record(resource_metadata):
    adapter = HydroshareMetadataAdapter()
    catalog_record = adapter.to_catalog_record(resource_metadata)

    # Convert date values to datetime objects
    created_date = datetime.fromisoformat(resource_metadata["created"].replace("Z", "+00:00"))
    modified_date = datetime.fromisoformat(resource_metadata["modified"].replace("Z", "+00:00"))
    published_date = datetime.fromisoformat(resource_metadata["published"].replace("Z", "+00:00"))
    start_date = datetime.fromisoformat(resource_metadata["period_coverage"]["start"].replace("Z", "+00:00"))

    assert catalog_record is not None
    assert catalog_record.additionalType == resource_metadata["type"]
    assert catalog_record.provider.name == "HYDROSHARE"
    assert catalog_record.name == "Virginia Forest - Soil water content, Soil temperature, Electrical conductivity - Jun 2021-Jul 2024"
    assert catalog_record.description == resource_metadata["abstract"]
    assert catalog_record.url == resource_metadata["url"]
    assert catalog_record.identifier[0] == resource_metadata["identifier"]
    # check number of creators
    assert len(catalog_record.creator) == len(resource_metadata["creators"])
    assert catalog_record.creator[0].name == resource_metadata["creators"][0]["name"]
    # check number of contributors
    assert len(catalog_record.contributor) == len(resource_metadata["contributors"])
    assert catalog_record.contributor[0].name == resource_metadata["contributors"][0]["name"]

    assert catalog_record.dateCreated == created_date
    assert catalog_record.dateModified == modified_date
    assert catalog_record.datePublished == published_date
    assert catalog_record.keywords == resource_metadata["subjects"]
    assert catalog_record.inLanguage == resource_metadata["language"]
    assert catalog_record.funding[0].name == resource_metadata["awards"][0]["title"]
    assert catalog_record.spatialCoverage.geo.box == "37.4677 -75.8294 37.4578 -75.8391"
    assert catalog_record.temporalCoverage.startDate == start_date
    assert catalog_record.associatedMedia == []
    assert catalog_record.isPartOf[0].name == resource_metadata["relations"][0]["value"]
    assert catalog_record.hasPart == []
    assert catalog_record.license.name == resource_metadata["rights"]["statement"]
    assert catalog_record.citation[0] == resource_metadata["citation"]
    assert catalog_record.creativeWorkStatus.name == "Published"
