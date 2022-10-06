import os
from pathlib import Path


async def list_files(path: str):
    urls = []
    files = []
    user_metadata = []
    
    for p in Path(path).rglob('*'):
        print(p)
'''
    for blob in client.list_blobs(bucketId):
        # TODO update special file types scanning to be more flexible
        if str(blob.name).endswith(".tif") or str(blob.name).endswith(".tiff") or str(blob.name).endswith(".vrt"):
            urls.append(f"https://extract-raster-service-edqqnq2wjq-uc.a.run.app/metadata?bucketId={bucketId}&objectId={blob.name}")
        elif str(blob.name).endswith(".shp"):
            urls.append(f"https://extract-feature-service-edqqnq2wjq-uc.a.run.app/metadata?bucketId={bucketId}&objectId={blob.name}")

        if str(blob.name).endswith("hs_user_meta.json"):
            user_metadata.append(blob)
        elif not str(blob.name).endswith("/"): # filter folders
            files.append(blob)
    
    # sort files with aggregations
    sorted_files = sorted(files, key=lambda i: (i.name, len(i.name.split("/"))))
    sorted_user_metadata = sorted(user_metadata, key=lambda i: (i.name, len(i.name.split("/"))))

    aggs = {}
    for um in sorted_user_metadata:
        agg_files = list(filter(lambda f: f.name.startswith(os.path.dirname(um.name)), sorted_files))
        aggs[um] = agg_files
        for f in agg_files:
            sorted_files.remove(f)

    collection = get_database()
    all_metadata = []
    checksums = []
    for um_blob, agg_files in aggs.items():
        print(um_blob.name)
        relations = []
        for f in agg_files:
            if f.name in relation_ids_by_filename:
                relations.append(relation_ids_by_filename[f.name])
        try:
            user_metadata_json = json.loads(um_blob.download_as_text())
        except Exception as e:
            raise Exception(f"{um_blob.name} has invalid json", e)

        # TODO this checksum is terrible, probalby should use path?
        checksum = hashlib.md5(bytes(f'{bucketId}/{um_blob.name[0:-len("/hs_user_meta.json")]}', 'utf-8')).hexdigest()

        if um_blob.name == "hs_user_meta.json":
            relations = relations + checksums
        else:
            checksums.append(checksum)

        relation_ids = [{"type": "hasPart", "value": relation} for relation in relations]
        json_metadata = {"relations": relation_ids, "bucket": bucketId, "_id": checksum, "id": checksum, "files": [to_file_entry(f) for f in agg_files]}

        if "relations" in user_metadata_json:
            all_relations =  user_metadata_json["relations"] + json_metadata["relations"]
            json_metadata = {**json_metadata, **user_metadata_json}
            json_metadata["relations"] = all_relations

        collection.update_one({"_id": json_metadata["_id"]}, {"$set": json_metadata}, upsert=True)
        all_metadata.append(json_metadata)
    
    return all_metadata
'''