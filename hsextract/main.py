import json
from asyncio import run as aiorun
from typing import Optional

import typer

from hsextract.utils import list_and_extract, extract_metadata, is_file_path, is_dir_path, save_metadata, is_url

app = typer.Typer()


def _extract_metadata(file_type: str, path: str, base_url: Optional[str] = None, generate_metadata_file: bool = False):
    if not is_file_path(path):
        return
    if base_url is not None and not is_url(base_url):
        return

    metadata_dict = extract_metadata(file_type, path, base_url)
    if metadata_dict is None:
        return
    if generate_metadata_file:
        save_metadata(path=path, metadata_dict=metadata_dict)
    else:
        print(json.dumps(metadata_dict, indent=2))


@app.command()
def feature(path: str, base_url: Optional[str] = None, generate_metadata_file: bool = False):
    _extract_metadata("feature", path, base_url, generate_metadata_file)


@app.command()
def raster(path: str, base_url: Optional[str] = None, generate_metadata_file: bool = False):
    _extract_metadata("raster", path, base_url, generate_metadata_file)


@app.command()
def reftimeseries(path: str, base_url: Optional[str] = None, generate_metadata_file: bool = False):
    _extract_metadata("reftimeseries", path, base_url, generate_metadata_file)


@app.command()
def timeseries(path: str, base_url: Optional[str] = None, generate_metadata_file: bool = False):
    _extract_metadata("timeseries", path, base_url, generate_metadata_file)


@app.command()
def timeseriescsv(path: str, base_url: Optional[str] = None, generate_metadata_file: bool = False):
    _extract_metadata("timeseries", path, base_url, generate_metadata_file)


@app.command()
def netcdf(path: str, base_url: Optional[str] = None, generate_metadata_file: bool = False):
    _extract_metadata("netcdf", path, base_url, generate_metadata_file)


async def _extract(path: str, user_metadata_filename: str, base_url: str):
    # generates metadata json files for all files types in the given path
    await list_and_extract(path, user_metadata_filename, base_url)


@app.command()
def extract(path: str, base_url: str, user_metadata_filename: str = "hs_user_meta.json"):
    if not is_dir_path(path):
        return
    aiorun(_extract(path, user_metadata_filename, base_url))


if __name__ == "__main__":
    app()
