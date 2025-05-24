import reranker
import yaml
import glob
import pandas as pd
import re
from box import ConfigBox
import os 
from pathlib import Path

with open("user/alex/reranking-config.yml", "r") as file:
    config_reranker = ConfigBox(yaml.safe_load(file))

import argparse

# target date is passed in as an argument
parser = argparse.ArgumentParser()
parser.add_argument("--date", type=str, required=True)
args = parser.parse_args()

target_date = args.date

ROOTPATHPARQUET = config_reranker.data.input_rootfolder

# load reranker
reranker_model = reranker.Rerank(config_reranker)
    

# run reranking
print(f'Running reranking for dataset: {target_date} now')
df = pd.read_parquet(f'{ROOTPATHPARQUET}/{target_date}/').sort_values(by=['qid', 'score'], ascending=[True, False]).reset_index().drop('index', axis=1)
   
result = reranker_model.rerank_stage(data=df)
# @Anthony, we can also merge them back together optionally, if needed
# result = pd.merge(df, result)
out_dir = Path(f"{config_reranker.data.output_folder}/{target_date}")
out_dir.mkdir(parents=True, exist_ok=True) 
result.to_csv(f'{out_dir}/reranked_results_{target_date}.csv')
    


