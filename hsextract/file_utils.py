import mimetypes
import os
from hsextract import s3
import hashlib
from functools import partial


def file_metadata(path: str):
    if path == "/tmp/hs_user_meta.json":
        return file_metadata_local(path)
    checksum = s3.checksum(path)
    size = f"{s3.info(path)['Size']/1000.00} KB"
    mime_type = mimetypes.guess_type(path)[0]
    _, extension = os.path.splitext(path)
    mime_type = mime_type if mime_type else extension
    _, name = os.path.split(path)
    return {
        "@type": "DataDownload",
        "name": name,
        "contentUrl": path,
        "contentSize": size,
        "sha256": checksum,
        "encodingFormat": mime_type,
    }, None

# temporaryily needed for local copy of hs_user_meta
def file_metadata_local(path: str):
    # path = "/files/" + path
    with open(path, "rb") as f:
        d = hashlib.sha256()
        for buf in iter(partial(f.read, 128), b''):
            d.update(buf)
    checksum = d.hexdigest()
    size = f"{os.path.getsize(path)/1000.00} KB"
    mime_type = mimetypes.guess_type(path)[0]
    _, extension = os.path.splitext(path)
    mime_type = mime_type if mime_type else extension
    _, name = os.path.split(path)
    return {
        "@type": "DataDownload",
        "name": name,
        "contentUrl": path,
        "contentSize": size,
        "sha256": checksum,
        "encodingFormat": mime_type,
    }, None