import xmltodict
import os

from osgeo import ogr, osr
from pathlib import Path
from bs4 import BeautifulSoup


UNKNOWN_STR = "unknown"
TITLE_MAX_LENGTH = 300


def extract_metadata_and_files(feature_path):
    shape_files, xml_file, _ = get_all_related_shp_files(feature_path)
    meta_dict = extract_metadata(feature_path)
    # meta_dict["url"] = shp_file.public_url
    # meta_dict["id"] = hashlib.md5(bytes(shp_file.public_url, 'utf-8')).hexdigest()
    # meta_dict["url"] = shp_file
    # meta_dict["id"] = hashlib.md5(bytes(shp_file, 'utf-8')).hexdigest()
    meta_dict = add_metadata(meta_dict, xml_file)
    files = []
    for f in shape_files:
        files.append(f)
    meta_dict["files"] = files
    return meta_dict


def get_all_related_shp_files(feature_path):
    shape_res_files = []
    xml_file = None
    dir_path = os.path.dirname(feature_path)
    for f in os.listdir(dir_path if dir_path else os.getcwd()):
        f_path = Path(f)
        if str(f_path.suffix).lower() == '.xml' and not str(f_path.name).lower().endswith('.shp.xml'):
            continue
        if str(f_path.suffix).lower() in [
            ".shp",
            ".shx",
            ".dbf",
            ".prj",
            ".sbx",
            ".sbn",
            ".cpg",
            ".xml",
            ".fbn",
            ".fbx",
            ".ain",
            ".aih",
            ".atx",
            ".ixs",
            ".mxs",
        ]:
            shape_res_files.append(os.path.join(dir_path, f))
        if str(f_path.suffix).lower() == ".xml":
            xml_file = os.path.join(dir_path, f)
        if str(f_path.suffix).lower() == ".shp":
            shp_file = os.path.join(dir_path, f)
    return shape_res_files, xml_file, shp_file


def extract_metadata(shp_file):
    """
    Collects metadata from a .shp file specified by *shp_file_full_path*
    :param shp_file_full_path:
    :return: returns a dict of collected metadata
    """

    metadata_dict = {}

    # wgs84 extent
    parsed_md_dict = parse_shp(shp_file)
    if parsed_md_dict["wgs84_extent_dict"]["westlimit"] != UNKNOWN_STR:
        wgs84_dict = parsed_md_dict["wgs84_extent_dict"]
        # if extent is a point, create point type coverage
        if wgs84_dict["westlimit"] == wgs84_dict["eastlimit"] and wgs84_dict["northlimit"] == wgs84_dict["southlimit"]:
            coverage_dict = {
                "type": "point",
                "east": wgs84_dict["eastlimit"],
                "north": wgs84_dict["northlimit"],
                "units": wgs84_dict["units"],
                "projection": wgs84_dict["projection"],
            }
        else:  # otherwise, create box type coverage
            coverage_dict = {"type": "box", **parsed_md_dict["wgs84_extent_dict"]}

        metadata_dict["spatial_coverage"] = coverage_dict

    # original extent
    original_coverage_dict = {
        "northlimit": parsed_md_dict["origin_extent_dict"]["northlimit"],
        "southlimit": parsed_md_dict["origin_extent_dict"]["southlimit"],
        "westlimit": parsed_md_dict["origin_extent_dict"]["westlimit"],
        "eastlimit": parsed_md_dict["origin_extent_dict"]["eastlimit"],
        "projection_string": parsed_md_dict["origin_projection_string"],
        "projection_name": parsed_md_dict["origin_projection_name"],
        "datum": parsed_md_dict["origin_datum"],
        "units": parsed_md_dict["origin_unit"],
    }

    metadata_dict["spatial_reference"] = original_coverage_dict

    # field
    field_info_array = []
    field_name_list = parsed_md_dict["field_meta_dict"]['field_list']
    for field_name in field_name_list:
        field_info_dict_item = parsed_md_dict["field_meta_dict"]["field_attr_dict"][field_name]
        field_info_array.append(field_info_dict_item)

    metadata_dict['field_information'] = field_info_array

    # geometry
    geometryinformation = {
        "feature_count": parsed_md_dict["feature_count"],
        "geometry_type": parsed_md_dict["geometry_type"],
    }

    metadata_dict["geometry_information"] = geometryinformation
    return metadata_dict


def parse_shp(shp_file_path):
    """
    :param shp_file_path: full file path fo the .shp file

    output dictionary format
    shp_metadata_dict["origin_projection_string"]: original projection string
    shp_metadata_dict["origin_projection_name"]: origin_projection_name
    shp_metadata_dict["origin_datum"]: origin_datum
    shp_metadata_dict["origin_unit"]: origin_unit
    shp_metadata_dict["field_meta_dict"]["field_list"]: list [fieldname1, fieldname2...]
    shp_metadata_dict["field_meta_dict"]["field_attr_dic"]:
       dict {"fieldname": dict {
                             "fieldName":fieldName,
                             "fieldTypeCode":fieldTypeCode,
                             "fieldType":fieldType,
                             "fieldWidth:fieldWidth,
                             "fieldPrecision:fieldPrecision"
                              }
             }
    shp_metadata_dict["feature_count"]: feature count
    shp_metadata_dict["geometry_type"]: geometry_type
    shp_metadata_dict["origin_extent_dict"]:
    dict{"west": east, "north":north, "east":east, "south":south}
    shp_metadata_dict["wgs84_extent_dict"]:
    dict{"west": east, "north":north, "east":east, "south":south}
    """

    shp_metadata_dict = {}
    # read shapefile
    driver = ogr.GetDriverByName('ESRI Shapefile')
    dataset = driver.Open(shp_file_path)

    # get layer
    layer = dataset.GetLayer()
    # get spatialRef from layer
    spatialRef_from_layer = layer.GetSpatialRef()

    if spatialRef_from_layer is not None:
        shp_metadata_dict["origin_projection_string"] = str(spatialRef_from_layer)
        prj_name = spatialRef_from_layer.GetAttrValue('projcs')
        if prj_name is None:
            prj_name = spatialRef_from_layer.GetAttrValue('geogcs')
        shp_metadata_dict["origin_projection_name"] = prj_name

        shp_metadata_dict["origin_datum"] = spatialRef_from_layer.GetAttrValue('datum')
        shp_metadata_dict["origin_unit"] = spatialRef_from_layer.GetAttrValue('unit')
    else:
        shp_metadata_dict["origin_projection_string"] = UNKNOWN_STR
        shp_metadata_dict["origin_projection_name"] = UNKNOWN_STR
        shp_metadata_dict["origin_datum"] = UNKNOWN_STR
        shp_metadata_dict["origin_unit"] = UNKNOWN_STR

    field_list = []
    filed_attr_dic = {}
    field_meta_dict = {"field_list": field_list, "field_attr_dict": filed_attr_dic}
    shp_metadata_dict["field_meta_dict"] = field_meta_dict
    # get Attributes
    layerDefinition = layer.GetLayerDefn()
    for i in range(layerDefinition.GetFieldCount()):
        fieldName = layerDefinition.GetFieldDefn(i).GetName()
        field_list.append(fieldName)
        attr_dict = {}
        field_meta_dict["field_attr_dict"][fieldName] = attr_dict

        attr_dict["field_name"] = fieldName
        fieldTypeCode = layerDefinition.GetFieldDefn(i).GetType()
        attr_dict["field_type_code"] = fieldTypeCode
        fieldType = layerDefinition.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)
        attr_dict["field_type"] = fieldType
        fieldWidth = layerDefinition.GetFieldDefn(i).GetWidth()
        attr_dict["field_width"] = fieldWidth
        fieldPrecision = layerDefinition.GetFieldDefn(i).GetPrecision()
        attr_dict["field_precision"] = fieldPrecision

    # get layer extent
    layer_extent = layer.GetExtent()

    # get feature count
    featureCount = layer.GetFeatureCount()
    shp_metadata_dict["feature_count"] = featureCount

    # get a feature from layer
    feature = layer.GetNextFeature()

    # get geometry from feature
    geom = feature.GetGeometryRef()

    # get geometry name
    shp_metadata_dict["geometry_type"] = geom.GetGeometryName()

    # reproject layer extent
    # source SpatialReference
    source = spatialRef_from_layer
    # target SpatialReference
    target = osr.SpatialReference()
    target.ImportFromEPSG(4326)

    # create two key points from layer extent
    left_upper_point = ogr.Geometry(ogr.wkbPoint)
    left_upper_point.AddPoint(layer_extent[0], layer_extent[3])  # left-upper
    right_lower_point = ogr.Geometry(ogr.wkbPoint)
    right_lower_point.AddPoint(layer_extent[1], layer_extent[2])  # right-lower

    # source map always has extent, even projection is unknown
    shp_metadata_dict["origin_extent_dict"] = {}
    shp_metadata_dict["origin_extent_dict"]["eastlimit"] = layer_extent[0]
    shp_metadata_dict["origin_extent_dict"]["westlimit"] = layer_extent[3]
    shp_metadata_dict["origin_extent_dict"]["southlimit"] = layer_extent[1]
    shp_metadata_dict["origin_extent_dict"]["northlimit"] = layer_extent[2]

    # reproject to WGS84
    shp_metadata_dict["wgs84_extent_dict"] = {}

    if source is not None:
        # define CoordinateTransformation obj
        transform = osr.CoordinateTransformation(source, target)
        # project two key points
        left_upper_point.Transform(transform)
        right_lower_point.Transform(transform)
        shp_metadata_dict["wgs84_extent_dict"]["northlimit"] = left_upper_point.GetX()
        shp_metadata_dict["wgs84_extent_dict"]["westlimit"] = left_upper_point.GetY()
        shp_metadata_dict["wgs84_extent_dict"]["southlimit"] = right_lower_point.GetX()
        shp_metadata_dict["wgs84_extent_dict"]["eastlimit"] = right_lower_point.GetY()
        shp_metadata_dict["wgs84_extent_dict"]["projection"] = "WGS 84 EPSG:4326"
        shp_metadata_dict["wgs84_extent_dict"]["units"] = "Decimal degrees"
    else:
        shp_metadata_dict["wgs84_extent_dict"]["westlimit"] = UNKNOWN_STR
        shp_metadata_dict["wgs84_extent_dict"]["northlimit"] = UNKNOWN_STR
        shp_metadata_dict["wgs84_extent_dict"]["eastlimit"] = UNKNOWN_STR
        shp_metadata_dict["wgs84_extent_dict"]["southlimit"] = UNKNOWN_STR
        shp_metadata_dict["wgs84_extent_dict"]["projection"] = UNKNOWN_STR
        shp_metadata_dict["wgs84_extent_dict"]["units"] = UNKNOWN_STR

    return shp_metadata_dict


def add_metadata(metadata_dict, xml_file):
    if xml_file:
        shp_xml_metadata_list = parse_shp_xml(xml_file)
        for shp_xml_metadata in shp_xml_metadata_list:
            if 'description' in shp_xml_metadata:
                metadata_dict["abstract"] = shp_xml_metadata['description']['abstract']

            elif 'title' in shp_xml_metadata:
                metadata_dict["title"] = shp_xml_metadata['title']['value']

            elif 'subject' in shp_xml_metadata:
                metadata_dict["subjects"] = [shp_xml_metadata['subject']['value']]
    return metadata_dict


def parse_shp_xml(xml_file):
    """
    Parse ArcGIS 10.X ESRI Shapefile Metadata XML. file to extract metadata for the following
    elements:
        title
        abstract
        keywords
    :param shp_xml_full_path: Expected fullpath to the .shp.xml file
    :return: a list of metadata dict
    """
    metadata = []
    with open(xml_file, "r") as f:
        xml_file_str = f.read()
    xml_dict = xmltodict.parse(xml_file_str)
    if 'dataIdInfo' in xml_dict['metadata']:
        dataIdInfo_dict = xml_dict['metadata']['dataIdInfo']
        if 'idCitation' in dataIdInfo_dict:
            if 'resTitle' in dataIdInfo_dict['idCitation']:
                if '#text' in dataIdInfo_dict['idCitation']['resTitle']:
                    title_value = dataIdInfo_dict['idCitation']['resTitle']['#text']
                else:
                    title_value = dataIdInfo_dict['idCitation']['resTitle']

                title_max_length = TITLE_MAX_LENGTH
                if len(title_value) > title_max_length:
                    title_value = title_value[: title_max_length - 1]
                title = {'title': {'value': title_value}}
                metadata.append(title)

        if 'idAbs' in dataIdInfo_dict:
            description_value = strip_tags(dataIdInfo_dict['idAbs'])
            description = {'description': {'abstract': description_value}}
            metadata.append(description)

        if 'searchKeys' in dataIdInfo_dict:
            searchKeys_dict = dataIdInfo_dict['searchKeys']
            if 'keyword' in searchKeys_dict:
                keyword_list = []
                if type(searchKeys_dict["keyword"]) is list:
                    keyword_list += searchKeys_dict["keyword"]
                else:
                    keyword_list.append(searchKeys_dict["keyword"])
                for k in keyword_list:
                    metadata.append({'subject': {'value': k}})
    return metadata


def strip_tags(value):
    return ''.join(BeautifulSoup(value, features="html.parser").findAll(text=True))
