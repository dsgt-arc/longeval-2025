from typer import Typer
from .etl import app as etl_app
from .opensearch import app as opensearch_app

app = Typer(no_args_is_help=True)
app.add_typer(etl_app, name="etl")
app.add_typer(opensearch_app, name="opensearch")
