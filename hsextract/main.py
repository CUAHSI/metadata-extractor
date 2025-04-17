from asyncio import run as aiorun

from hsextract.adapters.hydroshare import HydroshareMetadataAdapter
import typer

from hsextract.utils import list_and_extract
from typing_extensions import Annotated

app = typer.Typer()


async def _extract(
    input_path: str, output_path: str, input_base_url: str, output_base_url: str, user_metadata_filename: str
):
    await list_and_extract(input_path, output_path, input_base_url, output_base_url, user_metadata_filename)


@app.command()
def extract(
    input_path: str,
    output_path: str,
    retrieve_metadata_resource_id: Annotated[str, typer.Argument()] = None,
    output_base_url: Annotated[str, typer.Argument()] = "https://hydroshare.org/resource/extracted_metadata/data/contents",
    input_base_url: Annotated[str, typer.Argument()] = "https://hydroshare.org/resource",
    user_metadata_filename: Annotated[str, typer.Argument()] = "hs_user_meta.json",
):
    if retrieve_metadata_resource_id:
        adapter = HydroshareMetadataAdapter()
        adapter.retrieve_user_metadata(retrieve_metadata_resource_id, input_path)

    aiorun(_extract(input_path, output_path, input_base_url, output_base_url, user_metadata_filename))


if __name__ == "__main__":
    app()
