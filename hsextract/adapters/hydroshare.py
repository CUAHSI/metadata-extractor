import json
import os

from datetime import datetime
from typing import Any, List, Optional, Union

import requests
from pydantic import BaseModel, EmailStr, HttpUrl

from hsextract.adapters.utils import RepositoryType
from hsextract.exceptions import RepositoryException
from hsextract.models import schema
from hsextract.models.schema import CoreMetadataDOC


class Creator(BaseModel):
    name: Optional[str]
    email: Optional[EmailStr]
    organization: Optional[str]
    homepage: Optional[HttpUrl]
    address: Optional[str]
    identifiers: Optional[dict] = {}

    def to_dataset_creator(self):
        if self.name:
            creator = schema.Creator.construct()
            creator.name = self.name
            if self.email:
                creator.email = self.email
            if self.organization:
                affiliation = schema.Organization.construct()
                affiliation.name = self.organization
                creator.affiliation = affiliation
            _ORCID_identifier = self.identifiers.get("ORCID", "")
            if _ORCID_identifier:
                creator.identifier = _ORCID_identifier
        else:
            creator = schema.Organization.construct()
            creator.name = self.organization
            if self.homepage:
                creator.url = self.homepage
            if self.address:
                creator.address = self.address

        return creator


class Award(BaseModel):
    funding_agency_name: str
    title: Optional[str]
    number: Optional[str]
    funding_agency_url: Optional[HttpUrl]

    def to_dataset_grant(self):
        grant = schema.Grant.construct()
        if self.title:
            grant.name = self.title
        else:
            grant.name = self.funding_agency_name
        if self.number:
            grant.identifier = self.number

        funder = schema.Organization.construct()
        funder.name = self.funding_agency_name
        if self.funding_agency_url:
            funder.url = self.funding_agency_url

        grant.funder = funder
        return grant


class TemporalCoverage(BaseModel):
    start: datetime
    end: datetime

    def to_dataset_temporal_coverage(self):
        temp_cov = schema.TemporalCoverage.construct()
        if self.start:
            temp_cov.startDate = self.start
            if self.end:
                temp_cov.endDate = self.end
        return temp_cov


class SpatialCoverageBox(BaseModel):
    name: Optional[str]
    northlimit: float
    eastlimit: float
    southlimit: float
    westlimit: float

    def to_dataset_spatial_coverage(self):
        place = schema.Place.construct()
        if self.name:
            place.name = self.name

        place.geo = schema.GeoShape.construct()
        place.geo.box = f"{self.northlimit} {self.eastlimit} {self.southlimit} {self.westlimit}"
        return place


class SpatialCoveragePoint(BaseModel):
    name: Optional[str]
    north: float
    east: float

    def to_dataset_spatial_coverage(self):
        place = schema.Place.construct()
        if self.name:
            place.name = self.name
        place.geo = schema.GeoCoordinates.construct()
        place.geo.latitude = self.north
        place.geo.longitude = self.east
        return place


class ContentFile(BaseModel):
    path: str
    size: int
    mime_type: str = None
    checksum: str

    def to_dataset_media_object(self):
        media_object = schema.MediaObject.construct()
        media_object.contentUrl = self.path
        media_object.encodingFormat = self.mime_type
        media_object.contentSize = f"{self.size/1000.00} KB"
        media_object.name = self.path.split("/")[-1]
        return media_object


class Relation(BaseModel):
    type: str
    value: str

    def to_dataset_part_relation(self, relation_type: str):
        relation = None
        if relation_type == "IsPartOf" and self.type.endswith("is part of"):
            relation = schema.IsPartOf.construct()
        elif relation_type == "HasPart" and self.type.endswith("resource includes"):
            relation = schema.HasPart.construct()
        else:
            return relation

        description, url = self.value.rsplit(',', 1)
        relation.description = description.strip()
        relation.url = url.strip()
        relation.name = self.value if self.value else "No name found and is required"
        return relation


class Rights(BaseModel):
    statement: str
    url: HttpUrl

    def to_dataset_license(self):
        _license = schema.License.construct()
        _license.name = self.statement
        _license.url = self.url
        return _license


class HydroshareMetadataAdapter:
    @staticmethod
    def to_catalog_record(metadata: dict):
        """Converts hydroshare resource metadata to a catalog dataset record"""
        hs_metadata_model = _HydroshareResourceMetadata(**metadata)
        return hs_metadata_model.to_catalog_dataset()

    def retrieve_user_metadata(self, record_id: str, input_path: str):
        hs_meta_url = f"https://hydroshare.org/hsapi2/resource/{record_id}/json/"

        def make_request(url) -> Union[dict, List[dict]]:
            response = requests.get(url)
            if response.status_code != 200:
                raise RepositoryException(status_code=response.status_code, detail=response.text)
            return response.json()

        metadata = make_request(hs_meta_url)
        metadata = self.to_catalog_record(metadata).dict()
        with open(os.path.join(input_path, "hs_user_meta.json"), "w") as f:
            json.dump(metadata, f, indent=4, default=str)


class _HydroshareResourceMetadata(BaseModel):
    title: Optional[str]
    abstract: Optional[str]
    url: Optional[HttpUrl]
    identifier: Optional[HttpUrl]
    creators: List[Creator] = []
    created: Optional[datetime]
    modified: Optional[datetime]
    published: Optional[datetime]
    subjects: Optional[List[str]]
    language: Optional[str]
    rights: Optional[Rights]
    awards: List[Award] = []
    spatial_coverage: Optional[Union[SpatialCoverageBox, SpatialCoveragePoint]]
    period_coverage: Optional[TemporalCoverage]
    relations: List[Relation] = []
    citation: Optional[str]
    associatedMedia: List[Any] = []

    def to_dataset_creators(self):
        creators = []
        for creator in self.creators:
            creators.append(creator.to_dataset_creator())
        return creators

    def to_dataset_funding(self):
        grants = []
        for award in self.awards:
            grants.append(award.to_dataset_grant())
        return grants

    def to_dataset_associated_media(self):
        return self.associatedMedia

    def to_dataset_is_part_of(self):
        return self._to_dataset_part_relations("IsPartOf")

    def to_dataset_has_part(self):
        return self._to_dataset_part_relations("HasPart")

    def _to_dataset_part_relations(self, relation_type: str):
        part_relations = []
        for relation in self.relations:
            part_relation = relation.to_dataset_part_relation(relation_type)
            if part_relation:
                part_relations.append(part_relation)
        return part_relations

    def to_dataset_spatial_coverage(self):
        if self.spatial_coverage:
            return self.spatial_coverage.to_dataset_spatial_coverage()
        return None

    def to_dataset_period_coverage(self):
        if self.period_coverage:
            return self.period_coverage.to_dataset_temporal_coverage()
        return None

    def to_dataset_keywords(self):
        if self.subjects:
            return self.subjects
        return ["HydroShare"]

    def to_dataset_license(self):
        if self.rights:
            return self.rights.to_dataset_license()

    @staticmethod
    def to_dataset_provider():
        provider = schema.Organization.construct()
        provider.name = RepositoryType.HYDROSHARE
        provider.url = "https://www.hydroshare.org/"
        return provider

    def to_catalog_dataset(self):
        dataset = CoreMetadataDOC.construct()
        dataset.provider = self.to_dataset_provider()
        dataset.name = self.title
        dataset.description = self.abstract
        dataset.url = self.url
        dataset.identifier = [self.identifier]
        dataset.creator = self.to_dataset_creators()
        dataset.dateCreated = self.created
        dataset.dateModified = self.modified
        dataset.datePublished = self.published
        dataset.keywords = self.to_dataset_keywords()
        dataset.inLanguage = self.language
        dataset.funding = self.to_dataset_funding()
        dataset.spatialCoverage = self.to_dataset_spatial_coverage()
        dataset.temporalCoverage = self.to_dataset_period_coverage()
        dataset.associatedMedia = self.to_dataset_associated_media()
        dataset.isPartOf = self.to_dataset_is_part_of()
        dataset.hasPart = self.to_dataset_has_part()
        dataset.license = self.to_dataset_license()
        dataset.citation = [self.citation]
        return dataset
