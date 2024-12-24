import typer
from .parquet.workflow import main as parquet_main
from .tokens.workflow import main as tokens_main

app = typer.Typer()
app.command("parquet")(parquet_main)
app.command("tokens")(tokens_main)
