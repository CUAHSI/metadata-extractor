from datetime import datetime
from typing import Any, List, Optional, Union
from pydantic import BaseModel, EmailStr, HttpUrl
from hsextract.adapters.utils import RepositoryType
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
        relation.name = self.value
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


class Variable(BaseModel):
    name: str
    descriptive_name: Optional[str]
    unit: str
    type: str
    shape: str
    method: Optional[str]

    def to_aggregation_variable(self):
        _property_value = schema.PropertyValue.construct()
        _property_value.name = self.name
        _property_value.unitCode = self.unit
        _property_value.description = self.descriptive_name
        _property_value.measurementTechnique = self.method
        # creating a nested PropertyValue object to account for the shape field of the extracted variable metadata
        _property_value.value = schema.PropertyValue.construct()
        _property_value.value.name = "shape"
        _property_value.value.unitCode = self.type
        _property_value.value.value = self.shape
        return _property_value


class NetCDFAggregationMetadataAdapter:
    @staticmethod
    def to_catalog_record(aggr_metadata: dict):
        """Converts extracted netcdf aggregation metadata to a catalog dataset record"""
        nc_aggr_model = _NetCDFAggregationMetadata(**aggr_metadata)
        return nc_aggr_model.to_catalog_dataset()


class _NetCDFAggregationMetadata(BaseModel):
    title: str
    abstract: str
    subjects: Optional[List[str]]
    variables: List[Variable]
    spatial_coverage: Optional[Union[SpatialCoverageBox, SpatialCoveragePoint]]
    period_coverage: Optional[TemporalCoverage]
    # the extracted file (media object) metadata is already in schema.MediaObject format
    associatedMedia: Optional[List[schema.MediaObject]]

    def to_aggregation_spatial_coverage(self):
        if self.spatial_coverage:
            return self.spatial_coverage.to_dataset_spatial_coverage()
        return None

    def to_aggregation_period_coverage(self):
        if self.period_coverage:
            return self.period_coverage.to_dataset_temporal_coverage()
        return None

    def to_aggregation_keywords(self):
        if self.subjects:
            return self.subjects
        return None

    def to_catalog_dataset(self):
        aggregation_metadata = schema.NetCDFAggregationMetadata.construct()
        aggregation_metadata.name = self.title
        aggregation_metadata.description = self.abstract
        aggregation_metadata.keywords = self.to_aggregation_keywords()
        aggregation_metadata.spatialCoverage = self.to_aggregation_spatial_coverage()
        aggregation_metadata.temporalCoverage = self.to_aggregation_period_coverage()
        aggregation_metadata.variableMeasured = [v.to_aggregation_variable() for v in self.variables]
        aggregation_metadata.additionalProperty = []
        aggregation_metadata.associatedMedia = self.associatedMedia
        return aggregation_metadata


class BandInformation(BaseModel):
    name: str
    maximum_value: float
    minimum_value: float
    no_data_value: float

    def to_aggregation_band_as_additional_property(self):
        band = schema.PropertyValue.construct()
        band.name = self.name
        band.maxValue = self.maximum_value
        band.minValue = self.minimum_value
        band.value = schema.PropertyValue.construct()
        band.value.name = "no_data_value"
        band.value.value = self.no_data_value
        return band


class SpatialReference(BaseModel):
    projection_string: str
    projection: str
    datum: str
    eastlimit: float
    northlimit: float
    units: str

    def to_aggregation_spatial_reference_as_additional_property(self):
        spatial_reference = schema.PropertyValue.construct()
        spatial_reference.name = "spatial_reference"
        spatial_reference.value = []
        proj_str = schema.PropertyValue.construct()
        proj_str.name = "projection_string"
        proj_str.value = self.projection_string
        spatial_reference.value.append(proj_str)

        projection = schema.PropertyValue.construct()
        projection.name = "projection"
        projection.value = self.projection
        spatial_reference.value.append(projection)

        east_limit = schema.PropertyValue.construct()
        east_limit.name = "eastlimit"
        east_limit.value = self.eastlimit
        spatial_reference.value.append(east_limit)

        north_limit = schema.PropertyValue.construct()
        north_limit.name = "northlimit"
        north_limit.value = self.northlimit
        spatial_reference.value.append(north_limit)

        return spatial_reference


class _RasterAggregationMetadata(BaseModel):
    title: Optional[str]
    spatial_coverage: Optional[Union[SpatialCoverageBox, SpatialCoveragePoint]]
    period_coverage: Optional[TemporalCoverage]
    # the extracted file (media object) metadata is already in schema.MediaObject format
    associatedMedia: Optional[List[schema.MediaObject]]
    band_information: BandInformation
    cell_information: dict
    spatial_reference: SpatialReference

    def to_aggregation_cell_info_as_additional_properties(self):
        additional_properties = []
        if self.cell_information:
            for key, value in self.cell_information.items():
                prop = schema.PropertyValue.construct()
                prop.name = key
                prop.value = value
                additional_properties.append(prop)
        return additional_properties

    def to_aggregation_spatial_coverage(self):
        if self.spatial_coverage:
            aggr_spatial_coverage = self.spatial_coverage.to_dataset_spatial_coverage()
            if aggr_spatial_coverage:
                aggr_spatial_coverage.additionalProperty = [
                    self.spatial_reference.to_aggregation_spatial_reference_as_additional_property()]

            return aggr_spatial_coverage
        return None

    def to_aggregation_period_coverage(self):
        if self.period_coverage:
            return self.period_coverage.to_dataset_temporal_coverage()
        return None

    def to_catalog_dataset(self):
        aggregation_metadata = schema.RasterAggregationMetadata.construct()
        aggregation_metadata.name = self.title
        aggregation_metadata.spatialCoverage = self.to_aggregation_spatial_coverage()
        aggregation_metadata.temporalCoverage = self.to_aggregation_period_coverage()
        aggregation_metadata.additionalProperty = self.to_aggregation_cell_info_as_additional_properties()
        aggregation_metadata.additionalProperty.append(
            self.band_information.to_aggregation_band_as_additional_property())
        aggregation_metadata.associatedMedia = self.associatedMedia
        return aggregation_metadata


class RasterAggregationMetadataAdapter:
    @staticmethod
    def to_catalog_record(aggr_metadata: dict):
        """Converts extracted raster aggregation metadata to a catalog dataset record"""
        aggr_model = _RasterAggregationMetadata(**aggr_metadata)
        return aggr_model.to_catalog_dataset()
