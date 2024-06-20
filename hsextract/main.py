import typer
import json

from asyncio import run as aiorun

from hsextract.utils import list_and_extract, extract_metadata, is_file_path, is_dir_path, save_metadata


app = typer.Typer()


def _extract_metadata(path: str, file_type: str, generate_metadata_file: bool):
    if not is_file_path(path):
        return
    metadata_dict = extract_metadata(file_type, path)
    if metadata_dict is None:
        return
    if generate_metadata_file:
        save_metadata(path=path, metadata_dict=metadata_dict)
    else:
        print(json.dumps(metadata_dict, indent=2))


@app.command()
def feature(path: str, generate_metadata_file: bool = False):
    _extract_metadata(path, "feature", generate_metadata_file)


@app.command()
def raster(path: str, generate_metadata_file: bool = False):
    _extract_metadata(path, "raster", generate_metadata_file)


@app.command()
def reftimeseries(path: str, generate_metadata_file: bool = False):
    _extract_metadata(path, "reftimeseries", generate_metadata_file)


@app.command()
def timeseries(path: str, generate_metadata_file: bool = False):
    _extract_metadata(path, "timeseries", generate_metadata_file)


@app.command()
def timeseriescsv(path: str, generate_metadata_file: bool = False):
    _extract_metadata(path, "timeseries", generate_metadata_file)


@app.command()
def netcdf(path: str, generate_metadata_file: bool = False):
    _extract_metadata(path, "netcdf", generate_metadata_file)


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
