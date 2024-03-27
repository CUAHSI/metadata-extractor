from datetime import datetime
from typing import Any, List, Optional, Union
from pydantic import BaseModel, EmailStr, HttpUrl, validator
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

    @validator("homepage", pre=True)
    def validate_homepage(cls, v):
        if v:
            return v.strip()
        return None

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
    projection: Optional[str]
    units: Optional[str]

    def to_dataset_spatial_coverage(self):
        place = schema.Place.construct()
        if self.name:
            place.name = self.name

        place.geo = schema.GeoShape.construct()
        place.geo.box = f"{self.northlimit} {self.eastlimit} {self.southlimit} {self.westlimit}"
        place.additionalProperty = []
        if self.projection:
            projection = schema.PropertyValue.construct()
            projection.name = "projection"
            projection.value = self.projection
            place.additionalProperty.append(projection)
        if self.units:
            units = schema.PropertyValue.construct()
            units.name = "units"
            units.value = self.units
            place.additionalProperty.append(units)

        return place


class SpatialCoveragePoint(BaseModel):
    name: Optional[str]
    north: float
    east: float
    projection: Optional[str]
    units: Optional[str]

    def to_dataset_spatial_coverage(self):
        place = schema.Place.construct()
        if self.name:
            place.name = self.name
        place.geo = schema.GeoCoordinates.construct()
        place.geo.latitude = self.north
        place.geo.longitude = self.east
        place.additionalProperty = []
        if self.projection:
            projection = schema.PropertyValue.construct()
            projection.name = "projection"
            projection.value = self.projection
            place.additionalProperty.append(projection)
        if self.units:
            units = schema.PropertyValue.construct()
            units.name = "units"
            units.value = self.units
            place.additionalProperty.append(units)
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
    creator: Optional[Creator]
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
        aggregation_metadata.creator = self.creator.to_dataset_creator()
        aggregation_metadata.keywords = self.to_aggregation_keywords()
        aggregation_metadata.spatialCoverage = self.to_aggregation_spatial_coverage()
        aggregation_metadata.temporalCoverage = self.to_aggregation_period_coverage()
        aggregation_metadata.variableMeasured = [v.to_aggregation_variable() for v in self.variables]
        aggregation_metadata.additionalProperty = []
        aggregation_metadata.associatedMedia = self.associatedMedia
        return aggregation_metadata


class RasterBandInformation(BaseModel):
    name: str
    variable_name: str
    variable_unit: str
    maximum_value: float
    minimum_value: float
    no_data_value: float

    def to_aggregation_band_as_additional_property(self):
        band = schema.PropertyValue.construct()
        band.name = "bandInformation"
        band.maxValue = self.maximum_value
        band.minValue = self.minimum_value
        band.value = []
        band_name = schema.PropertyValue.construct()
        band_name.name = "name"
        band_name.value = self.name
        band.value.append(band_name)
        band_no_data = schema.PropertyValue.construct()
        band_no_data.name = "noDataValue"
        band_no_data.value = self.no_data_value
        band.value.append(band_no_data)
        variable_name = schema.PropertyValue.construct()
        variable_name.name = "variableName"
        variable_name.value = self.variable_name
        band.value.append(variable_name)
        variable_unit = schema.PropertyValue.construct()
        variable_unit.name = "variableUnit"
        variable_unit.value = self.variable_unit
        band.value.append(variable_unit)
        return band


class RasterSpatialReference(BaseModel):
    projection_string: str
    projection: str
    datum: str
    eastlimit: float
    northlimit: float
    westlimit: Optional[float]
    southlimit: Optional[float]
    units: str

    def to_aggregation_spatial_reference_as_additional_property(self, coverage_type: str):
        spatial_reference = schema.PropertyValue.construct()
        spatial_reference.name = "spatialReference"
        spatial_reference.unitCode = self.units
        spatial_reference.value = []

        datum = schema.PropertyValue.construct()
        datum.name = "datum"
        datum.value = self.datum
        spatial_reference.value.append(datum)

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
        if coverage_type == "box":
            west_limit = schema.PropertyValue.construct()
            west_limit.name = "westlimit"
            west_limit.value = self.westlimit
            spatial_reference.value.append(west_limit)

            south_limit = schema.PropertyValue.construct()
            south_limit.name = "southlimit"
            south_limit.value = self.southlimit
            spatial_reference.value.append(south_limit)

        return spatial_reference


class RasterCellInformation(BaseModel):
    name: str
    cell_data_type: str
    cell_size_x_value: float
    cell_size_y_value: float
    columns: int
    rows: int

    def to_aggregation_cell_info_as_additional_properties(self):
        cell_info = schema.PropertyValue.construct()
        cell_info.name = "cellInformation"
        cell_info.value = []

        cell_name = schema.PropertyValue.construct()
        cell_name.name = "name"
        cell_name.value = self.name
        cell_info.value.append(cell_name)

        cell_data_type = schema.PropertyValue.construct()
        cell_data_type.name = "cellDataType"
        cell_data_type.value = self.cell_data_type
        cell_info.value.append(cell_data_type)

        cell_size_x = schema.PropertyValue.construct()
        cell_size_x.name = "cellSizeXValue"
        cell_size_x.value = self.cell_size_x_value
        cell_info.value.append(cell_size_x)

        cell_size_y = schema.PropertyValue.construct()
        cell_size_y.name = "cellSizeYValue"
        cell_size_y.value = self.cell_size_y_value
        cell_info.value.append(cell_size_y)

        columns = schema.PropertyValue.construct()
        columns.name = "columns"
        columns.value = self.columns
        cell_info.value.append(columns)

        rows = schema.PropertyValue.construct()
        rows.name = "rows"
        rows.value = self.rows
        cell_info.value.append(rows)
        return cell_info


class _RasterAggregationMetadata(BaseModel):
    title: Optional[str]
    spatial_coverage: Optional[Union[SpatialCoverageBox, SpatialCoveragePoint]]
    period_coverage: Optional[TemporalCoverage]
    # the extracted file (media object) metadata is already in schema.MediaObject format
    associatedMedia: Optional[List[schema.MediaObject]]
    band_information: RasterBandInformation
    cell_information: RasterCellInformation
    spatial_reference: RasterSpatialReference

    def to_aggregation_spatial_coverage(self):
        if self.spatial_coverage:
            coverage_type = "box" if isinstance(self.spatial_coverage, SpatialCoverageBox) else "point"
            aggr_spatial_coverage = self.spatial_coverage.to_dataset_spatial_coverage()
            if aggr_spatial_coverage:
                spatial_reference = self.spatial_reference.to_aggregation_spatial_reference_as_additional_property(
                    coverage_type=coverage_type)
                aggr_spatial_coverage.additionalProperty.append(spatial_reference)

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
        aggregation_metadata.additionalProperty = []
        aggregation_metadata.additionalProperty.append(
            self.cell_information.to_aggregation_cell_info_as_additional_properties())
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


class FieldInformation(BaseModel):
    field_name: str
    field_precision: int
    field_type: str
    field_type_code: int
    field_width: int

    def to_aggregation_field_info_as_additional_property(self, field_info):
        field_name = schema.PropertyValue.construct()
        field_name.name = "fieldName"
        field_name.value = self.field_name
        field_info.value.append(field_name)

        field_precision = schema.PropertyValue.construct()
        field_precision.name = "fieldPrecision"
        field_precision.value = self.field_precision
        field_info.value.append(field_precision)

        field_type = schema.PropertyValue.construct()
        field_type.name = "fieldType"
        field_type.value = self.field_type
        field_info.value.append(field_type)

        field_type_code = schema.PropertyValue.construct()
        field_type_code.name = "fieldTypeCode"
        field_type_code.value = self.field_type_code
        field_info.value.append(field_type_code)

        field_width = schema.PropertyValue.construct()
        field_width.name = "fieldWidth"
        field_width.value = self.field_width
        field_info.value.append(field_width)


class GeometryInformation(BaseModel):
    feature_count: int
    geometry_type: str

    def to_aggregation_geometry_info_as_additional_property(self):
        geometry_info = schema.PropertyValue.construct()
        geometry_info.name = "geometryInformation"
        geometry_info.value = []

        feature_count = schema.PropertyValue.construct()
        feature_count.name = "featureCount"
        feature_count.value = self.feature_count
        geometry_info.value.append(feature_count)

        geometry_type = schema.PropertyValue.construct()
        geometry_type.name = "geometryType"
        geometry_type.value = self.geometry_type
        geometry_info.value.append(geometry_type)
        return geometry_info


class FeatureSpatialReference(BaseModel):
    projection_string: str
    projection_name: str
    datum: str
    eastlimit: float
    northlimit: float
    westlimit: Optional[float]
    southlimit: Optional[float]
    units: str

    def to_aggregation_spatial_reference_as_additional_property(self, coverage_type: str):
        spatial_reference = schema.PropertyValue.construct()
        spatial_reference.name = "spatialReference"
        spatial_reference.unitCode = self.units
        spatial_reference.value = []

        datum = schema.PropertyValue.construct()
        datum.name = "datum"
        datum.value = self.datum
        spatial_reference.value.append(datum)

        proj_str = schema.PropertyValue.construct()
        proj_str.name = "projection_string"
        proj_str.value = self.projection_string
        spatial_reference.value.append(proj_str)

        projection = schema.PropertyValue.construct()
        projection.name = "projection_name"
        projection.value = self.projection_name
        spatial_reference.value.append(projection)

        east_limit = schema.PropertyValue.construct()
        east_limit.name = "eastlimit"
        east_limit.value = self.eastlimit
        spatial_reference.value.append(east_limit)

        north_limit = schema.PropertyValue.construct()
        north_limit.name = "northlimit"
        north_limit.value = self.northlimit
        spatial_reference.value.append(north_limit)
        if coverage_type == "box":
            west_limit = schema.PropertyValue.construct()
            west_limit.name = "westlimit"
            west_limit.value = self.westlimit
            spatial_reference.value.append(west_limit)

            south_limit = schema.PropertyValue.construct()
            south_limit.name = "southlimit"
            south_limit.value = self.southlimit
            spatial_reference.value.append(south_limit)
        return spatial_reference


class _FeatureAggregationMetadata(BaseModel):
    title: Optional[str]
    abstract: Optional[str]
    spatial_coverage: Optional[Union[SpatialCoverageBox, SpatialCoveragePoint]]
    period_coverage: Optional[TemporalCoverage]
    # the extracted file (media object) metadata is already in schema.MediaObject format
    associatedMedia: Optional[List[schema.MediaObject]]
    geometry_information: GeometryInformation
    field_information: List[FieldInformation]
    spatial_reference: FeatureSpatialReference

    def to_aggregation_spatial_coverage(self):
        if self.spatial_coverage:
            coverage_type = "box" if isinstance(self.spatial_coverage, SpatialCoverageBox) else "point"
            aggr_spatial_coverage = self.spatial_coverage.to_dataset_spatial_coverage()
            if aggr_spatial_coverage:
                spatial_reference = self.spatial_reference.to_aggregation_spatial_reference_as_additional_property(
                    coverage_type=coverage_type)
                aggr_spatial_coverage.additionalProperty.append(spatial_reference)

            return aggr_spatial_coverage
        return None

    def to_aggregation_period_coverage(self):
        if self.period_coverage:
            return self.period_coverage.to_dataset_temporal_coverage()
        return None

    def to_aggregation_field_info_as_additional_property(self):
        field_info = schema.PropertyValue.construct()
        field_info.name = "fieldInformation"
        field_info.value = []
        for field in self.field_information:
            field.to_aggregation_field_info_as_additional_property(field_info=field_info)
        return field_info

    def to_catalog_dataset(self):
        aggregation_metadata = schema.FeatureAggregationMetadata.construct()
        aggregation_metadata.name = self.title
        aggregation_metadata.description = self.abstract
        aggregation_metadata.spatialCoverage = self.to_aggregation_spatial_coverage()
        aggregation_metadata.temporalCoverage = self.to_aggregation_period_coverage()
        aggregation_metadata.additionalProperty = []
        aggregation_metadata.additionalProperty.append(self.to_aggregation_field_info_as_additional_property())
        aggregation_metadata.additionalProperty.append(
            self.geometry_information.to_aggregation_geometry_info_as_additional_property())
        aggregation_metadata.associatedMedia = self.associatedMedia
        return aggregation_metadata


class FeatureAggregationMetadataAdapter:
    @staticmethod
    def to_catalog_record(aggr_metadata: dict):
        """Converts extracted feature aggregation metadata to a catalog dataset record"""
        aggr_model = _FeatureAggregationMetadata(**aggr_metadata)
        return aggr_model.to_catalog_dataset()
