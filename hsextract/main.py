import typer
import json

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


if __name__ == "__main__":
    app()