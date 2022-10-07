# metadata-extractor

### Build the Docker Image
```shell
docker build -t hsextract .
abs_path=/Users/scootna/projects/metadata-extractor/tests/test_files
```

### Run directory extraction
```shell
docker run -v $abs_path:/files hsextract extract /files

cat $abs_path/.hs/output.json
```

### Run single file extraction
```shell
docker run -v $abs_path:/files hsextract extract /files/watersheds/watersheds.shp

cat $abs_path/.hs/output.json
```

### Run feature (.shp) extraction
```shell
docker run -v $abs_path:/files hsextract feature /files/watersheds/watersheds.shp
```

### Run raster (.vrt) extraction
```shell
docker run -v $abs_path:/files hsextract raster /files/rasters/logan.vrt
```

### Run netcdf (.nc) extraction
```shell
docker run -v $abs_path:/files hsextract netcdf /files/netcdf/netcdf_valid.nc
```

### Run timeseries (.sqlite) extraction
```shell
docker run -v $abs_path:/files hsextract timeseries /files/timeseries/ODM2_Multi_Site_One_Variable.sqlite
```

### Run timeseries (.csv) extraction
```shell
docker run -v $abs_path:/files hsextract timeseriescsv /files/timeseries/ODM2_Multi_Site_One_Variable_Test.csv
```

### Run referenced timeseries (.refts.json) extraction
```shell
docker run -v $abs_path:/files hsextract reftimeseries /files/reftimeseries/multi_sites_formatted_version1.0.refts.json
```
