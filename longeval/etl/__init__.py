import typer
from .dev_sample.workflow import main as dev_sample_main
from .parquet.workflow import main as parquet_main
from .tokens.workflow import main as tokens_main
from .opensearch.workflow import main as opensearch_main

app = typer.Typer()
app.command("dev-sample")(dev_sample_main)
app.command("parquet")(parquet_main)
app.command("tokens")(tokens_main)
app.command("opensearch")(opensearch_main)
