import typer
from .dev_sample.workflow import main as dev_sample_main
from .parquet.workflow import to_parquet
from .tokens.workflow import count_tokens
from .embedding.workflow import main as embedding_main
from .lda.workflow import main as lda_main
from .nmf.workflow import main as nmf_main

app = typer.Typer(no_args_is_help=True)
app.command("dev-sample")(dev_sample_main)
app.command("parquet")(to_parquet)
app.command("tokens")(count_tokens)
app.command("embedding")(embedding_main)
app.command("lda")(lda_main)
app.command("nmf")(nmf_main)

