import s3fs
import os

s3 = s3fs.S3FileSystem(endpoint_url=os.environ["AWS_S3_ENDPOINT"])
