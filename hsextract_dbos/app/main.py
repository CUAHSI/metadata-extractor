import json
import os

import uvicorn
from fastapi import FastAPI
from hsextract.hs_cn_schemas.schema.src.base import MediaObject, HasPart
from enum import Enum
from .s3_utils import exists, find, retrieve_file_manifest, write_metadata, load_metadata, delete_metadata


app = FastAPI()


class ContentType(Enum):
    RASTER = "raster"
    NETCDF = "netcdf"
    ZARR = "zarr"
    FEATURE = "feature"
    REFTIMESERIES = "reftimeseries"
    TIMESERIES = "timeseries"
    UNKNOWN = "unknown"
    SINGLE_FILE = "single_file"
    FILE_SET = "file_set"

class MetadataObject:
    def __init__(self, file_object_path: str, file_updated: bool, resource_contents_path: str = None, resource_md_path: str = None, resource_md_jsonld_path: str = None):
        self.file_object_path = file_object_path
        self.file_updated = file_updated
        
        bucket_name = file_object_path.split('/')[0]
        resource_id = file_object_path.split('/')[1]
        if not resource_contents_path:
            resource_contents_path = f"{bucket_name}/{resource_id}/data/contents"
        if not resource_md_jsonld_path:
            resource_md_jsonld_path = f"{bucket_name}/{resource_id}/.hsjsonld"
        if not resource_md_path:
            resource_md_path = f"{bucket_name}/{resource_id}/.hs"

        self.resource_contents_path = resource_contents_path # bucket/resource_id/data/contents
        self.resource_md_path = resource_md_path # bucket/resource_id/.hs
        self.resource_md_jsonld_path = resource_md_jsonld_path # bucket/resource_id/.hs/jsonld
        self.content_type_md_jsonld_path = None # content type metadata path, e.g. bucket/.md/resource_id/content_type.json
        self._resource_associated_media = None
        self.system_metadata_path = os.path.join(self.resource_md_path, "system_metadata.json")
        self.user_metadata_path = os.path.join(self.resource_contents_path, "hs_user_meta.json")
        self.resource_metadata_path = os.path.join(self.resource_md_jsonld_path, "dataset_metadata.json")
        self.content_type = self.determine_content_type()
        self._determine_paths()

    def _determine_paths(self):
        # content_type_md_path
        parent_directory = os.path.dirname(self.file_object_path)
        relative_path = os.path.relpath(parent_directory, self.resource_contents_path)
        # TODO check directory content types (e.g. fileset, zarr)
        if self.content_type == ContentType.FILE_SET:
            self.content_type_md_jsonld_path = os.path.join(self.resource_md_jsonld_path, relative_path, "dataset_metadata.json")
            self.content_type_md_path = os.path.join(self.resource_md_path, relative_path, "dataset_metadata.json")
            self.content_type_contents_path = os.path.join(self.resource_contents_path, relative_path)
            self.content_type_main_file_path = os.path.join(self.resource_contents_path, relative_path)
            # TODO make this a file in the .hs metadata directory
            self.content_type_md_user_path = os.path.join(self.resource_contents_path, relative_path, "hs_user_meta.json")
        # check all other content types
        elif self.content_type != ContentType.UNKNOWN:
            relative_path = os.path.relpath(self.file_object_path, self.resource_contents_path)
            self.content_type_md_jsonld_path = os.path.join(self.resource_md_jsonld_path, relative_path + ".json")
            self.content_type_md_path = os.path.join(self.resource_md_path, relative_path + ".json")
            self.content_type_contents_path = None # os.path.join(self.resource_root_path, relative_path)
            # assuming all are main files for now
            self.content_type_main_file_path = os.path.join(self.resource_contents_path, relative_path)
            self.content_type_md_user_path = os.path.join(self.resource_contents_path, relative_path + ".hs_user_meta.json")
    
    @property
    def resource_associated_media(self):
        if not self._resource_associated_media:
            self._resource_associated_media = retrieve_file_manifest(self.resource_contents_path)
        return self._resource_associated_media

    def content_type_associated_media(self) -> list[MediaObject]:
        """
        Get a list of media objects associated with this resource.
        """
        media_objects = []
        if self.content_type in [ContentType.SINGLE_FILE, ContentType.NETCDF, ContentType.REFTIMESERIES, ContentType.TIMESERIES]:
            return [m for m in self.resource_associated_media if m["contentUrl"].endswith(self.file_object_path)]
        elif self.content_type in [ContentType.FILE_SET, ContentType.ZARR]:
            return [m for m in self.resource_associated_media if m["contentUrl"].split(os.environ['AWS_S3_ENDPOINT'])[1].strip("/").startswith(self.content_type_contents_path)]
        return media_objects

    def extract_metadata(self) -> dict:
        if self.content_type == ContentType.NETCDF:
            from hsextract.netcdf.hs_cn_extraction import encode_netcdf
            metadata = encode_netcdf(self.file_object_path).model_dump(exclude_none=True)
        elif self.content_type == ContentType.RASTER:
            from hsextract.raster.hs_cn_extraction import encode_raster_metadata
            metadata = encode_raster_metadata(self.file_object_path).model_dump(exclude_none=True)
        elif self.content_type == ContentType.FEATURE:
            from hsextract.feature.hs_cn_extraction import encode_vector_metadata
            metadata = encode_vector_metadata(self.file_object_path).model_dump(exclude_none=True)
        if self.content_type == ContentType.TIMESERIES:
            from hsextract.timeseries.utils import extract_metadata
            metadata = extract_metadata(self.file_object_path).model_dump(exclude_none=True)
        else:
            return
        write_metadata(self.content_type_md_path, metadata)
    
    _extension_mapping = {
        ".tif": ContentType.RASTER,
        ".tiff": ContentType.RASTER,
        ".vrt": ContentType.RASTER,
        ".nc": ContentType.NETCDF,
        #".zarr": ContentType.ZARR,
        ".shp": ContentType.FEATURE,
        #".reftst.json": ContentType.REFTIMESERIES,
        ".csv": ContentType.TIMESERIES,
        ".sqlite": ContentType.TIMESERIES,
    }
    
    def determine_content_type(self) -> ContentType:
        """
        Determines the content type of the file based on its extension.
        """
        _, extension = os.path.splitext(self.file_object_path.lower())
        content_type = self._extension_mapping.get(extension, ContentType.UNKNOWN)

        if content_type == ContentType.UNKNOWN:
            # check singlefile
            single_file_user_path = self.file_object_path + ".hs_user_meta.json"
            if exists(single_file_user_path):
                return ContentType.SINGLE_FILE
            # check fileset
            parent_directory = os.path.dirname(self.file_object_path)
            while parent_directory:
                file_set_user_path = os.path.join(parent_directory, "hs_user_meta.json")
                if file_set_user_path == self.user_metadata_path:
                    break
                if exists(file_set_user_path):
                    return ContentType.FILE_SET
                parent_directory = os.path.dirname(parent_directory)

        if content_type == ContentType.UNKNOWN:
            # TODO: determine if other steps are necessary
            pass

        return content_type


@app.get("/metadata_extraction")
def launch_durable_workflow(file_object_path: str = "sblack/40d20c1496544ad8b7bf6bfa46695890/data/contents/dataset.csv",
                            file_updated: bool = True,
                            resource_root_path: str = None,
                            resource_md_root_path: str = None,
                            resource_md_cache_path: str = None) -> None:
    return workflow_metadata_extraction(file_object_path, file_updated, resource_root_path, resource_md_root_path, resource_md_cache_path)

from pydantic import BaseModel

class MinIOEvent(BaseModel):
    EventName: str
    Key: str

@app.post("/minio_event")
def handle_minio_event(event: MinIOEvent) -> None:
    workflow_metadata_extraction(event.Key, event.EventName == "s3:ObjectCreated:Put")

def determine_required_for_content_type(file_object_path: str, content_type: ContentType) -> bool:
    # For fileset and single file, it only matters if it is the hs_user_meta.json file
    if file_object_path.endswith("hs_user_meta.json"):
        return True
    
    return True
    with s3.open(content_type_reference, 'r') as s3_file:
        content_type_metadata = json.loads(s3_file.read())
    print(f"Content type metadata: {content_type_metadata}")
    
    _, extension = os.path.splitext(file_object_path)
    if extension in [".vrt", ".tiff", ".tif", ".nc", ".zarr", ".shp"]: #TODO make list exhaustive for all content types
        return True
    return False


def write_resource_metadata(md: MetadataObject) -> bool:
    # TODO; do all the reads asynchronously
    # read the system metadata file
    system_json = load_metadata(md.system_metadata_path)

    # read the resource metadata hs_user_meta.json file
    user_json = load_metadata(md.user_metadata_path)

    # generate content type hasPart relationships
    content_type_metadata_paths: list[str] = [file for file in find(md.resource_md_path) if file != f"{md.resource_md_path}/dataset_metadata.json"]
    has_parts = []

    for file in content_type_metadata_paths:
        print(f"Processing content type metadata file: {file}")
        content_type_metadata = load_metadata(file)
        
        file_prefix = '/'.join(file.split('/')[1:])  # Remove the bucket name from the path
        has_part = HasPart( # TODO: probably need content type here as well for driving the landing page
            name=content_type_metadata.get("name", "Not Found and name is required"),
            description=content_type_metadata.get("description", None),
            url= f"{os.environ['AWS_S3_ENDPOINT']}/{file_prefix}",
        )
        has_parts.append(has_part.model_dump(exclude_none=True))

    # Combine system metadata, user metadata, hasPart, and associatedMedia
    combined_metadata = {**system_json, **user_json} #TODO evaluate whether we need to merge list properties
    combined_metadata["hasPart"] = has_parts
    combined_metadata["associatedMedia"] = md.resource_associated_media

    # Write the combined metadata to the resource metadata file
    print(f"Writing resource metadata to: {md.resource_metadata_path}")
    write_metadata(md.resource_metadata_path, combined_metadata)

def write_content_type_metadata(md: MetadataObject) -> bool:
    # read the part metadata file
    part_json = {}
    if md.content_type_md_path:
        part_json = load_metadata(md.content_type_md_path)

    # read the content type user metadata file
    user_json = {}
    if md.content_type_md_user_path:
        user_json = load_metadata(md.content_type_md_user_path)

    # generate content type isPartOf relationships
    resource_md_prefix = '/'.join(md.resource_md_path.split('/')[1:])
    is_part_of = [f"{os.environ['AWS_S3_ENDPOINT']}/{resource_md_prefix}"] # TODO: determine whether to use IsPartOf

    content_type_associated_media = md.content_type_associated_media()

    # Combine part metadata, user metadata, isPartOf, and associatedMedia
    combined_metadata = {**part_json, **user_json} #TODO evaluate whether we need to merge list properties
    combined_metadata["isPartOf"] = is_part_of
    combined_metadata["associatedMedia"] = content_type_associated_media

    # Write the combined metadata to the resource metadata file
    write_metadata(md.content_type_md_jsonld_path, combined_metadata)

def workflow_metadata_extraction(file_object_path: str, file_updated: bool = True, resource_contents_path: str = None, resource_md_path: str = None, resource_md_jsonld_path: str = None) -> None: # if a file is not updated, it is deleted
    md = MetadataObject(file_object_path, file_updated)
    print(f"content type determined: {md.content_type}")
    # fileset and single file do not have anything to extract
    if md.content_type != ContentType.UNKNOWN:
        if file_updated:
            md.extract_metadata()
            write_content_type_metadata(md)
        else:
            delete_metadata(md.content_type_md_path)

    print("Writing resource metadata")
    write_resource_metadata(md)
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
