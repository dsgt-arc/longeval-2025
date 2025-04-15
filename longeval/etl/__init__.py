import typer
from .dev_sample.workflow import main as dev_sample_main
from .parquet.workflow import to_parquet
from .embedding.workflow import main as embedding_main
from .lda.workflow import main as lda_main

app = typer.Typer(no_args_is_help=True)
app.command("dev-sample")(dev_sample_main)
app.command("parquet")(to_parquet)
app.command("embedding")(embedding_main)
app.command("lda")(lda_main)
