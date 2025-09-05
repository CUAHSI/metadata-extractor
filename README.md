# metadata-extractor

### Build the Docker Image
```shell
docker build -t hsextract .
```

### Add the envionrment configuration for S3 access
```shell
AWS_ACCESS_KEY_ID=key
AWS_SECRET_ACCESS_KEY=secret
AWS_S3_ENDPOINT=endpoint
```

### Start the fastapi server
```shell
docker-compose up -d
```

Navigate to localhost:8000/docs to see the endpoint. Only the first parameter is required and the default value will work on beta minio