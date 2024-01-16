import hashlib
import os
import mimetypes

from functools import partial


def file_metadata(path: str):
    with open(path, "rb") as f:
        d = hashlib.md5()
        for buf in iter(partial(f.read, 128), b''):
            d.update(buf)
    checksum = d.hexdigest()
    size = os.path.getsize(path)
    mime_type = mimetypes.guess_type(path)[0]
    # path = path[:]
    return {"path": path, "size": size, "checksum": checksum, "mime_type": mime_type}, None
