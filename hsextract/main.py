from asyncio import run as aiorun

import typer

from hsextract.utils import list_and_extract

app = typer.Typer()


async def _extract(
    input_path: str, output_path: str, input_base_url: str, output_base_url: str, user_metadata_filename: str
):
    await list_and_extract(input_path, output_path, input_base_url, output_base_url, user_metadata_filename)


@app.command()
def extract(
    input_path: str,
    output_path: str,
    input_base_url: str = "https://hydroshare.org/resource/resource_id/data/contents/",
    output_base_url: str = "https://hydroshare.org/resource/resource_id/data/extracted_metadata",
    user_metadata_filename: str = "hs_user_meta.json",
):
    aiorun(_extract(input_path, output_path, input_base_url, output_base_url, user_metadata_filename))


if __name__ == "__main__":
    app()
