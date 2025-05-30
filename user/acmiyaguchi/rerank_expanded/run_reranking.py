from pathlib import Path
import argparse

import pandas as pd
import reranker
import yaml
from box import ConfigBox


with (Path(__file__).parent / "reranking-config.yml").open("r") as file:
    config_reranker = ConfigBox(yaml.safe_load(file))


# target date is passed in as an argument
parser = argparse.ArgumentParser()
parser.add_argument("--date", type=str, required=True)
parser.add_argument(
    "--overwrite", action="store_true", help="Overwrite existing results"
)
args = parser.parse_args()

target_date = args.date

# check for the output file
out_path = Path(config_reranker.data.output_folder) / target_date / "results.csv"
if out_path.exists() and not args.overwrite:
    print(f"Results for {target_date} already exist, skipping reranking.")
    exit(0)

ROOTPATHPARQUET = config_reranker.data.input_rootfolder

# load reranker
reranker_model = reranker.Rerank(config_reranker)


# run reranking
print(f"Running reranking for dataset: {target_date} now")
df = (
    pd.read_parquet((Path(ROOTPATHPARQUET) / target_date).as_posix())
    .sort_values(by=["qid", "score"], ascending=[True, False])
    .reset_index()
    .drop("index", axis=1)
)

# if contents is empty, then drop the row
df = df[df["contents"].str.len() > 0].reset_index(drop=True)

result = reranker_model.rerank_stage(data=df)
out_path.parent.mkdir(parents=True, exist_ok=True)
result.to_csv(out_path)
