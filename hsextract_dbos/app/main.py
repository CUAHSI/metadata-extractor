import json
import os

import uvicorn
from dbos import DBOS, DBOSConfig
from fastapi import FastAPI
from hsextract.hs_cn_schemas.schema.src.base import MediaObject, HasPart
from enum import Enum
from .s3_utils import exists, find, retrieve_file_manifest, write_metadata, load_metadata, s3_client


app = FastAPI()
config: DBOSConfig = {
    "name": "hydroshare_metadata_extraction",
    "database_url": os.environ.get("DBOS_DATABASE_URL"),
}
DBOS(fastapi=app, config=config, conductor_key=os.environ.get("DBOS_CONDUCTOR_KEY"))

steps_event = "steps_event"
delete_event = "delete_event"
update_event = "update_event"

class ContentType(Enum):
    RASTER = "raster"
    NETCDF = "netcdf"
    ZARR = "zarr"
    FEATURE = "feature"
    REFTIMESERIES = "reftimeseries"
    TIMESERIES = "timeseries"
    USER_META = "user_meta"
    SYSTEM_META = "system_meta"
    UNKNOWN = "unknown"
    SINGLE_FILE = "single_file"
    FILE_SET = "file_set"

class MetadataObject:
    def __init__(self, file_object_path: str, file_updated: bool, resource_root_path: str, resource_md_root_path: str, resource_part_root_path: str):
        self.file_object_path = file_object_path
        self.file_updated = file_updated
        self.resource_root_path = resource_root_path # bucket/resource_id/data/contents
        self.resource_md_root_path = resource_md_root_path # bucket/md/resource_id
        self.resource_part_root_path = resource_part_root_path # bucket/.md/resource_id
        self.content_type_md_path = None # content type metadata path, e.g. bucket/.md/resource_id/content_type.json
        self._determine_paths()
        self.resource_associated_media = retrieve_file_manifest(self.resource_root_path)

    def _determine_paths(self):
        # content_type_md_path
        # check single file
        file_object_path = self.file_object_path
        single_file_user_path = file_object_path + ".hs_user_meta.json"
        resource_user_metadata_path = os.path.join(self.resource_root_path, "hs_user_meta.json")

        # Extract the relative path from file_object_path after resource_root_path
        relative_path = os.path.relpath(self.resource_root_path, file_object_path)
        self.content_type_md_path = None
        if self.file_object_path == resource_user_metadata_path:
            return
        if exists(os.path.join(single_file_user_path)):
            self.content_type_md_path = os.path.join(self.resource_md_root_path, file_object_path + ".json")
            self.content_type = ContentType.SINGLE_FILE
        else:
            # check fileset
            file_object_directory_full_path = os.path.dirname(file_object_path)
            parent_directory = file_object_directory_full_path
            while parent_directory:
                print(f"parent directory : {parent_directory}")
                file_set_user_path = os.path.join(parent_directory, "hs_user_meta.json")
                if exists(file_set_user_path):
                    print(f"resource_root_path {self.resource_root_path}")
                    relative_path = os.path.relpath(parent_directory, self.resource_root_path)
                    print(f"relative path : {relative_path}")
                    self.content_type_md_path = os.path.join(self.resource_md_root_path, relative_path, "dataset_metadata.json")
                    self.content_type_root_path = os.path.join(self.resource_root_path, relative_path)
                    self.content_type = ContentType.FILE_SET
                    break
                parent_directory = os.path.dirname(parent_directory)
        # TODO: other content types        
        
    @property
    def resource_md_path(self) -> str:
        return os.path.join(self.resource_md_root_path, "dataset_metadata.json")
    
    @property
    def content_type_user_md_path(self) -> str:
        content_type_md = self.content_type_md_path
        relative_path = os.path.relpath(content_type_md, self.resource_md_root_path)
        if content_type_md.endswith("dataset_metadata.json"):
            fileset_user_metadata_path = os.path.join(content_type_md, relative_path, "hs_user_meta.json")
            return fileset_user_metadata_path
        return os.path.join(content_type_md, relative_path + ".hs_user_meta.json")
    
    @property
    def part_md_path(self) -> str:
        content_type_md = self.content_type_md_path
        relative_path = os.path.relpath(content_type_md, self.resource_md_root_path)
        return os.path.join(self.resource_part_root_path, relative_path)

    def content_type_associated_media(self) -> list[MediaObject]:
        """
        Get a list of media objects associated with this resource.
        """
        media_objects = []
        if self.content_type in [ContentType.SINGLE_FILE, ContentType.NETCDF, ContentType.REFTIMESERIES, ContentType.TIMESERIES]:
            print(f"content type file_object_path {self.file_object_path}")
            
            return [m for m in self.resource_associated_media if m["contentUrl"].endswith(self.file_object_path)]
        elif self.content_type in [ContentType.FILE_SET, ContentType.ZARR]:
            return [m for m in self.resource_associated_media if m["contentUrl"].split(os.environ['AWS_S3_ENDPOINT'])[1].strip("/").startswith(self.content_type_root_path)]
        return media_objects


@app.get("/metadata_extraction")
def launch_durable_workflow(file_object_path: str = "sblack/40d20c1496544ad8b7bf6bfa46695890/data/contents/.csv",
                            file_updated: bool = True,
                            resource_root_path: str = None,
                            resource_md_root_path: str = None,
                            resource_md_part_path: str = None) -> None:
    bucket_name = file_object_path.split('/')[0]
    resource_id = file_object_path.split('/')[1]
    if not resource_root_path:
        resource_root_path = f"{bucket_name}/{resource_id}/data/contents"
    if not resource_md_root_path:
        resource_md_root_path = f"{bucket_name}/md/{resource_id}"
    if not resource_md_part_path:
        resource_md_part_path = f"{bucket_name}/.md/{resource_id}"
    handle = DBOS.start_workflow(workflow_metadata_extraction, file_object_path, file_updated, resource_root_path, resource_md_root_path, resource_md_part_path)
    # Wait for the background task to complete and retrieve its result.
    succeeded = handle.get_result()
    return succeeded


@DBOS.step()
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


@DBOS.step()
def write_resource_metadata(md: MetadataObject) -> bool:
    # TODO; do all the reads asynchronously
    # read the system metadata file
    print(f"Reading system metadata from: {md.resource_md_root_path}/system_metadata.json")
    system_metadata_path = f"{md.resource_md_root_path}/system_metadata.json"
    system_json = load_metadata(system_metadata_path)

    print("reading resource user metadata from: hs_user_meta.json")
    # read the resource metadata hs_user_meta.json file
    user_metadata_path = f"{md.resource_root_path}/hs_user_meta.json"
    user_json = load_metadata(user_metadata_path)

    # generate content type hasPart relationships
    print(f"Generating hasPart relationships for content type metadata in: {md.resource_md_root_path}")
    content_type_metadata_paths: list[str] = [file for file in find(md.resource_md_root_path) if file != f"{md.resource_md_root_path}/dataset_metadata.json"]
    has_parts = []
    for file in content_type_metadata_paths:
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
    print(f"Writing combined metadata to: {md.resource_md_path}")
    write_metadata(md.resource_md_path, combined_metadata)

@DBOS.step()
def write_content_type_metadata(md: MetadataObject) -> bool:
    # read the part metadata file
    part_json = {}
    print("1")
    content_type_part_metadata_path = md.part_md_path
    print("2")
    if content_type_part_metadata_path:
        print("3")
        part_json = load_metadata(content_type_part_metadata_path)
    print(f"loaded part_json {part_json}")

    # read the content type user metadata file
    user_json = {}
    content_type_user_metadata_path = md.content_type_user_md_path
    if content_type_user_metadata_path:
        user_json = load_metadata(content_type_user_metadata_path)
    print(f"loaded user_json {user_json}")

    # generate content type isPartOf relationships
    resource_md_prefix = '/'.join(md.resource_md_path.split('/')[1:])
    is_part_of = [f"{os.environ['AWS_S3_ENDPOINT']}/{resource_md_prefix}"] # TODO: determine whether to use IsPartOf

    content_type_associated_media = md.content_type_associated_media()

    # Combine part metadata, user metadata, isPartOf, and associatedMedia
    combined_metadata = {**part_json, **user_json} #TODO evaluate whether we need to merge list properties
    combined_metadata["isPartOf"] = is_part_of
    combined_metadata["associatedMedia"] = content_type_associated_media
    print(f"Writing content type combined metadata to: {md.content_type_md_path}")

    # Write the combined metadata to the resource metadata file
    write_metadata(md.content_type_md_path, combined_metadata)

@DBOS.workflow()
def workflow_metadata_extraction(file_object_path: str, file_updated: bool, resource_root_path: str, resource_md_root_path: str, resource_part_root_path: str) -> None: # if a file is not updated, it is deleted
    file_manifest: list[MediaObject] = retrieve_file_manifest(resource_root_path)
    #print(f"File manifest: {file_manifest}")
    DBOS.set_event(steps_event, 1)
    md = MetadataObject(file_object_path, file_updated, resource_root_path, resource_md_root_path, resource_part_root_path)
    content_type_md_path = md.content_type_md_path
    print(f"Content type md path: {content_type_md_path}")
    DBOS.set_event(steps_event, 2)
    if content_type_md_path and content_type_md_path != os.path.join(resource_md_root_path, "dataset_metadata.json"): # is not resource metadata
        # fileset and single file do not have anything to extract
        required = False # only supporting fileset and single file for now
        if required:
            if file_updated:
                # TODO extract content type metadata from the file object path to a content type part file
                pass
            else:
                # TODO delete content type metadata file
                pass
        print("writing content type md " + content_type_md_path)
        write_content_type_metadata(md)
    DBOS.set_event(steps_event, 3)
    write_resource_metadata(md)

if __name__ == "__main__":
    DBOS.launch()
    uvicorn.run(app, host="0.0.0.0", port=8000)
