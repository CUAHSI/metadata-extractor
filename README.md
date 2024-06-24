# metadata-extractor

### Build the Docker Image
```shell
docker build -t hsextract .
abs_path=/Users/scootna/projects/metadata-extractor/tests/test_files
```

### Run the Docker container with necessary permissions
```shell
docker run -it -p 9000:9000 --cap-add SYS_ADMIN --device /dev/fuse --entrypoint /bin/bash hsextract
```

### Docker shell commands
```shell
s3fs demo-composite /s3 -o passwd_file=/etc/s3cred,use_path_request_style,url=https://minio-api.cuahsi.io
python3 /app/hsextract/main.py extract /s3
```

### Run directory extraction (for all content types)
```shell
# using the base url for content file location in repository as https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents 
# the generated metadata files will be saved in the /files/.hs directory for all content types (feature, raster, netcdf, timeseries, reftimeseries)
docker run -v $abs_path:/files hsextract extract /files https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents

```

### Run directory extraction (for a specific content type - assuming the specified directory contains only files of one content type)
```shell
# using the base url for content file location in repository as https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents 
# the generated metadata files will be saved in the /files/netcdf/.hs directory for netcdf content type
docker run -v $abs_path:/files hsextract extract /files/netcdf https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents

```


### Run feature (.shp) extraction (extraction from a single file/content type)
```shell
# this will generate metadata for the specified shapefile and print the metadata to the console
docker run -v $abs_path:/files hsextract feature /files/watersheds/watersheds.shp

# to generate metadata for a shapefile and save the metadata to a file use the --generate-metadata-file option
# this will generate metadata for file at /files/watersheds/.hs/watersheds.shp.json
docker run -v $abs_path:/files hsextract feature /files/watersheds/watersheds.shp --gereate-metadata-file

# to use a repository content file location url in the generated metadata, use the --base-url option
docker run -v $abs_path:/files hsextract feature /files/watersheds/watersheds.shp --base-url https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents
```

### Run raster (.vrt) extraction (extraction from a single file/content type)
```shell
# this will generate metadata for the specified raster and print the metadata to the console
docker run -v $abs_path:/files hsextract raster /files/rasters/logan.vrt

# to generate metadata for a raster and save the metadata to a file use the --generate-metadata-file option
# this will generate metadata for file at /files/rasters/.hs/logan.vrt.json
docker run -v $abs_path:/files hsextract raster /files/rasters/logan.vrt --generate-metadata-file

# to use a repository content file location url in the generated metadata, use the --base-url option
docker run -v $abs_path:/files hsextract raster /files/rasters/logan.vrt --base-url https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents
```

### Run netcdf (.nc) extraction (extraction from a single file/content type)
```shell
# this will generate metadata for the specified netcdf and print the metadata to the console
docker run -v $abs_path:/files hsextract netcdf /files/netcdf/netcdf_valid.nc

# to generate metadata for a netcdf and save the metadata to a file use the --generate-metadata-file option
# this will generate metadata for file at /files/netcdf/.hs/netcdf_valid.nc.json
docker run -v $abs_path:/files hsextract netcdf /files/netcdf/netcdf_valid.nc --generate-metadata-file

# to use a repository content file location url in the generated metadata, use the --base-url option
docker run -v $abs_path:/files hsextract netcdf /files/netcdf/netcdf_valid.nc --base-url https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents

```

### Run timeseries (.sqlite) extraction (extraction from a single file/content type)
```shell
# this will generate metadata for the specified timeseries and print the metadata to the console
docker run -v $abs_path:/files hsextract timeseries /files/timeseries/ODM2_Multi_Site_One_Variable.sqlite

# to generate metadata for a timeseries and save the metadata to a file use the --generate-metadata-file option
# this will generate metadata for file at /files/timeseries/.hs/ODM2_Multi_Site_One_Variable.sqlite.json
docker run -v $abs_path:/files hsextract timeseries /files/timeseries/ODM2_Multi_Site_One_Variable.sqlite --generate-metadata-file

# to use a repository content file location url in the generated metadata, use the --base-url option
docker run -v $abs_path:/files hsextract timeseries /files/timeseries/ODM2_Multi_Site_One_Variable.sqlite --base-url https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents

```

### Run timeseries (.csv) extraction (extraction from a single file/content type)
```shell
# this will generate metadata for the specified timeseries and print the metadata to the console
docker run -v $abs_path:/files hsextract timeseriescsv /files/timeseries/ODM2_Multi_Site_One_Variable_Test.csv

# to generate metadata for a timeseries and save the metadata to a file use the --generate-metadata-file option
# this will generate metadata for file at /files/timeseries/.hs/ODM2_Multi_Site_One_Variable_Test.csv.json
docker run -v $abs_path:/files hsextract timeseriescsv /files/timeseries/ODM2_Multi_Site_One_Variable_Test.csv --generate-metadata-file

# to use a repository content file location url in the generated metadata, use the --base-url option
docker run -v $abs_path:/files hsextract timeseriescsv /files/timeseries/ODM2_Multi_Site_One_Variable_Test.csv --base-url https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents

```

### Run referenced timeseries (.refts.json) extraction (extraction from a single file/content type)
```shell
# this will generate metadata for the specified referenced timeseries and print the metadata to the console
docker run -v $abs_path:/files hsextract reftimeseries /files/reftimeseries/multi_sites_formatted_version1.0.refts.json

# to generate metadata for a referenced timeseries and save the metadata to a file use the --generate-metadata-file option
# this will generate metadata for file at /files/reftimeseries/.hs/multi_sites_formatted_version1.0.refts.json.json
docker run -v $abs_path:/files hsextract reftimeseries /files/reftimeseries/multi_sites_formatted_version1.0.refts.json --generate-metadata-file

# to use a repository content file location url in the generated metadata, use the --base-url option
docker run -v $abs_path:/files hsextract reftimeseries /files/reftimeseries/multi_sites_formatted_version1.0.refts.json --base-url https://www.hydroshare.org/resource/1e4b3b3b7b7b4b3b8/data/contents

```
