import typer

app = typer.Typer()


@app.command()
def feature(path: str):
    print(f"Hello {path}")

@app.command()
def raster(path: str):
    print(f"Hello {path}")


if __name__ == "__main__":
    app()