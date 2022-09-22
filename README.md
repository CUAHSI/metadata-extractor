# metadata-extractor

### Build the Docker Image
```shell
docker build -t hsextract .
```

### Run feature extraction
```shell
watersheds_abs_path=/Users/scootna/projects/metadata-extractor/tests/test_files/watersheds
docker run -v $watersheds_abs_path:/files hsextract feature /files/watersheds.shp
```