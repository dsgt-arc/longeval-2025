import typer
from .dev_sample.workflow import main as dev_sample_main
from .parquet.workflow import to_parquet, to_trec_parquet
from .embedding.workflow import main as embedding_main
from .lda.workflow import (
    main as lda_main,
    sweep_main as lda_sweep_main,
    topic_proportions_main as lda_topic_proportions_main,
)
from .lda.heldout import heldout_drift_main as lda_heldout_drift_main
from .lda.convprobe import conv_probe_main as lda_conv_probe_main
from .nmf.workflow import main as nmf_main
from .irds.workflow import irds_parquet

app = typer.Typer(no_args_is_help=True)
app.command("dev-sample")(dev_sample_main)
app.command("parquet")(to_parquet)
app.command("trec-parquet")(to_trec_parquet)
app.command("irds-parquet")(irds_parquet)
app.command("embedding")(embedding_main)
app.command("lda")(lda_main)
app.command("lda-sweep")(lda_sweep_main)
app.command("lda-topic-proportions")(lda_topic_proportions_main)
app.command("lda-heldout-drift")(lda_heldout_drift_main)
app.command("lda-conv-probe")(lda_conv_probe_main)
app.command("nmf")(nmf_main)

