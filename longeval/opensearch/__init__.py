import typer

from .index.workflow import main as index_main

app = typer.Typer(no_args_is_help=True)
app.command("index")(index_main)
