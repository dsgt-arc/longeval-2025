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

ROOTPATHPARQUET = config_reranker.data.input_rootfolder

dates = set()
for f in os.listdir(ROOTPATHPARQUET):
    match = re.search(r"(date=\d{4}-\d{2})", f) #match the date format that they put up
    if match:
        dates.add(match.group(1)) 

reranker_model = reranker.Rerank(config_reranker)
    
for date in list(dates):
    print(f'Running reranking for dataset: {date} now')
    df = pd.read_parquet(f'{ROOTPATHPARQUET}/{date}/').sort_values(by=['qid', 'score'], ascending=[True, False]).reset_index().drop('index', axis=1)
   
    result = reranker_model.rerank_stage(data=df)
    # @Anthony, we can also merge them back together optionally, if needed
    # result = pd.merge(df, result)
    out_dir = Path(f"{config_reranker.data.output_folder}/{date}")
    out_dir.mkdir(parents=True, exist_ok=True) 
    result.to_csv(f'{out_dir}/reranked_results_{date}.csv')
    


