from asyncio import run as aiorun

from hsextract.adapters.hydroshare import HydroshareMetadataAdapter
import typer

from hsextract.utils import list_and_extract
from typing_extensions import Annotated

app = typer.Typer()


async def _extract(
    input_path: str, output_path: str, user_metadata_filename: str, local_output: bool
):
    await list_and_extract(input_path, output_path, user_metadata_filename, local_output)


@app.command()
def extract(
    input_path: str,
    output_path: str,
    retrieve_metadata_resource_id: Annotated[str, typer.Argument()] = None,
    user_metadata_filename: Annotated[str, typer.Argument()] = "hs_user_meta.json",
    local_output: Annotated[bool, typer.Option()] = False,
):
    if retrieve_metadata_resource_id:
        adapter = HydroshareMetadataAdapter()
        adapter.retrieve_user_metadata(retrieve_metadata_resource_id, input_path)

    aiorun(_extract(input_path, output_path, user_metadata_filename, local_output))


if __name__ == "__main__":
    app()
