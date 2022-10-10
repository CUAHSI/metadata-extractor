import typer
import json
import os

from asyncio import run as aiorun

from hsextract.utils import list_and_extract, extract_metadata


app = typer.Typer()


@app.command()
def feature(path: str):
    metadata_dict = extract_metadata("feature", path)
    print(json.dumps(metadata_dict, indent=2))

@app.command()
def raster(path: str):
    metadata_dict = extract_metadata("raster", path)
    print(json.dumps(metadata_dict, indent=2))

@app.command()
def reftimeseries(path: str):
    metadata_dict = extract_metadata("reftimeseries", path)
    print(json.dumps(metadata_dict, indent=2))

@app.command()
def timeseries(path: str):
    metadata_dict = extract_metadata("timeseries", path)
    print(json.dumps(metadata_dict, indent=2))    

@app.command()
def timeseriescsv(path: str):
    metadata_dict = extract_metadata("timeseries", path)
    print(json.dumps(metadata_dict, indent=2))

@app.command()
def netcdf(path: str):
    metadata_dict = extract_metadata("netcdf", path)
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