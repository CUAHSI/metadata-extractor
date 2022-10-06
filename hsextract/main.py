from hsextract.utils import list_and_extract
import typer
import json
import os

from asyncio import run as aiorun
from hsextract.feature.utils import extract_metadata_and_files
from hsextract.raster.utils import extract_from_tif_file

app = typer.Typer()


@app.command()
def feature(path: str):
    metadata_dict = extract_metadata_and_files(path)
    print(json.dumps(metadata_dict, indent=2))

@app.command()
def raster(path: str):
    metadata_dict = extract_from_tif_file(path)
    print(json.dumps(metadata_dict, indent=2))

async def _extract(path: str):
    sorted_files, extracted_metadata = await list_and_extract(path)
    all_metadata = {"files": sorted_files, "extracted": extracted_metadata}
    os.makedirs(f"{path}/.hs", exist_ok=True)
    with open(f"{path}/.hs/output.json", "w") as f:
        f.write(json.dumps(all_metadata, indent=2))

@app.command()
def extract(path: str):
    aiorun(_extract(path))


if __name__ == "__main__":
    app()