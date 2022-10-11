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

async def _extract(path: str):
    await list_and_extract(path)

@app.command()
def extract(path: str):
    aiorun(_extract(path))


if __name__ == "__main__":
    app()