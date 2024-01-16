import typer
import json

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


async def _extract(path: str, user_metadata_filename: str):
    await list_and_extract(path, user_metadata_filename)


@app.command()
def extract(path: str, user_metadata_filename: str = "hs_user_meta.json"):
    aiorun(_extract(path, user_metadata_filename))


if __name__ == "__main__":
    app()
