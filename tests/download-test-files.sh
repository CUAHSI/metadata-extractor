mkdir test_files

gsutil -m cp -r \
  "gs://demo_hydroshare_resource/hs_user_meta.json" \
  "gs://demo_hydroshare_resource/rasters" \
  "gs://demo_hydroshare_resource/readme.txt" \
  "gs://demo_hydroshare_resource/states" \
  "gs://demo_hydroshare_resource/watersheds" \
  "gs://demo_hydroshare_resource/netcdf" \
  "gs://demo_hydroshare_resource/reftimeseries" \
  test_files/