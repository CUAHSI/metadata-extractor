import typer
import json
import os

from asyncio import run as aiorun

from hsextract.utils import list_and_extract
from hsextract.feature.utils import extract_metadata_and_files
from hsextract.raster.utils import extract_from_tif_file
from hsextract.netcdf.utils import get_nc_meta_dict
from hsextract.reftimeseries.utils import extract_referenced_timeseries_metadata
from hsextract.timeseries.utils import extract_metadata as extract_timeseries_metadata, extract_metadata_csv

app = typer.Typer()


@app.command()
def feature(path: str):
    metadata_dict = extract_metadata_and_files(path)
    print(json.dumps(metadata_dict, indent=2))

@app.command()
def raster(path: str):
    metadata_dict = extract_from_tif_file(path)
    print(json.dumps(metadata_dict, indent=2))

@app.command()
def reftimeseries(path: str):
    metadata_dict = extract_referenced_timeseries_metadata(path)
    print(json.dumps(metadata_dict, indent=2))

@app.command()
def timeseries(path: str):
    metadata_dict = extract_timeseries_metadata(path)
    print(json.dumps(metadata_dict, indent=2))    

@app.command()
def timeseriescsv(path: str):
    metadata_dict = extract_metadata_csv(path)
    print(json.dumps(metadata_dict, indent=2))

@app.command()
def netcdf(path: str):
    metadata_dict = get_nc_meta_dict(path)
    print(json.dumps(metadata_dict, indent=2))

async def _extract(path: str, write: bool):
    sorted_files, extracted_metadata = await list_and_extract(path)
    all_metadata = {"files": sorted_files, "extracted": extracted_metadata}
    all_metadata = json.dumps(all_metadata, indent=2)
    '''
    if write:
        if os.path.isdir(path):
            os.makedirs(f"{path}/.hs", exist_ok=True)
            with open(f"{path}/.hs/output.json", "w") as f:
                f.write(all_metadata)
        else:
            with open(f".hs/output.json", "w") as f:
                f.write(all_metadata)
    else:
    '''
    print(all_metadata)

@app.command()
def extract(path: str, write=True):
    aiorun(_extract(path, write))


if __name__ == "__main__":
    app()