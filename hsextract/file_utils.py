import hashlib
import mimetypes
import os
from functools import partial
from typing import Optional


def file_metadata(path: str, base_url: Optional[str] = None):
    # path = "/files/" + path
    with open(path, "rb") as f:
        d = hashlib.sha256()
        for buf in iter(partial(f.read, 128), b''):
            d.update(buf)
    checksum = d.hexdigest()
    size = f"{os.path.getsize(path)/1000.00} KB"
    mime_type = mimetypes.guess_type(path)[0]
    file_name = os.path.basename(path)
    # strip the mount location from the path - assuming files are volume mounted at /files
    if path.startswith("/files/"):
        path = path[7:]
    if base_url is not None:
        base_url = base_url.rstrip("/")
        path = path.lstrip("/")
        file_url = os.path.join(base_url, path)
    else:
        file_url = path

    file_meta = {
        "contentUrl": file_url,
        "contentSize": size,
        "sha256": checksum,
        "encodingFormat": mime_type,
        "name": file_name
    }
    return file_meta, None
