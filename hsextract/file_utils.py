import mimetypes
import os
from hsextract import s3


def file_metadata(path: str):
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
