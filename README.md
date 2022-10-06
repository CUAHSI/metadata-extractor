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

### Run raster extraction
```shell
rasters_abs_path=/Users/scootna/projects/metadata-extractor/tests/test_files/rasters
docker run -v $rasters_abs_path:/files hsextract raster /files/logan.vrt
```

### Run extraction
```shell
abs_path=/Users/scootna/projects/metadata-extractor/tests/test_files
docker run -v $abs_path:/files hsextract extract /files/test_files
```