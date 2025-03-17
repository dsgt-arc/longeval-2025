import typer
from .dev_sample.workflow import main as dev_sample_main
from .parquet.workflow import main as parquet_main
from .tokens.workflow import main as tokens_main
from .embedding.workflow import main as embedding_main
from .lda.workflow import main as lda_main

app = typer.Typer(no_args_is_help=True)
app.command("dev-sample")(dev_sample_main)
app.command("parquet")(parquet_main)
app.command("tokens")(tokens_main)
app.command("embedding")(embedding_main)
app.command("lda")(lda_main)
