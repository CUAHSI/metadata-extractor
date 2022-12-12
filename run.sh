echo "$MINIO_KEY:$MINIO_SECRET_KEY" > passwd && chmod 600 passwd
s3fs "$S3_BUCKET" "$MNT_POINT" -o passwd_file=passwd,use_path_request_style,url=https://minio-api.cuahsi.io  && tail -f /dev/null