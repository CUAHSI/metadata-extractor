import typer
import json

from asyncio import run as aiorun

from hsextract.utils import list_and_extract, extract_metadata


app = typer.Typer()


@app.command()
def feature(path: str, base_url: str):
    metadata_dict = extract_metadata("feature", path, base_url)
    print(json.dumps(metadata_dict, indent=2))


@app.command()
def raster(path: str, base_url: str):
    metadata_dict = extract_metadata("raster", path, base_url)
    print(json.dumps(metadata_dict, indent=2))


@app.command()
def reftimeseries(path: str, base_url: str):
    metadata_dict = extract_metadata("reftimeseries", path, base_url)
    print(json.dumps(metadata_dict, indent=2))


@app.command()
def timeseries(path: str, base_url: str):
    metadata_dict = extract_metadata("timeseries", path, base_url)
    print(json.dumps(metadata_dict, indent=2))


@app.command()
def timeseriescsv(path: str, base_url: str):
    metadata_dict = extract_metadata("timeseries", path, base_url)
    print(json.dumps(metadata_dict, indent=2))


@app.command()
def netcdf(path: str, base_url: str):
    metadata_dict = extract_metadata("netcdf", path, base_url)
    print(json.dumps(metadata_dict, indent=2))


async def _extract(path: str, user_metadata_filename: str, base_url: str):
    await list_and_extract(path, user_metadata_filename, base_url)


@app.command()
def extract(path: str, base_url: str, user_metadata_filename: str = "hs_user_meta.json"):
    aiorun(_extract(path, user_metadata_filename, base_url))


if __name__ == "__main__":
    app()
