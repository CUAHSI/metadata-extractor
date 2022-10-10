import hashlib
import os
import mimetypes


def file_metadata(path: str):
    with open(path, "rb") as f:
        contents = f.read()
    checksum = hashlib.md5(contents).hexdigest()
    size = os.path.getsize(path)
    mime_type = mimetypes.guess_type(path)[0]
    return {
        "path": path,
        "size": size,
        "checksum": checksum,
        "mime_type": mime_type
    }