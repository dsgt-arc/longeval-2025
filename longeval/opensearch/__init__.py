import typer

from .index.workflow import main as index_main
from .snapshot import app as snapshot_app
from .admin import app as admin_app

app = typer.Typer(no_args_is_help=True)
app.command("index")(index_main)
app.add_typer(snapshot_app, name="snapshot")
app.add_typer(admin_app, name="admin")
