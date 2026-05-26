from dotenv import load_dotenv
from openai import OpenAI
import logging
import os
from box import ConfigBox
import yaml
import pandas as pd
import json
from tqdm import tqdm
import sqlite3
import time

class OpenAIQueryExpansion:
    def __init__(self, config):
        self.prompt_path = None
        self.config = config

        load_dotenv()
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    def load_original_queries(self, limit=None):
        self.dbconn = sqlite3.connect(self.config.files.queries_path_dir_french)
        if limit is not None:
            sql_query = f"SELECT id as query_id, query as text FROM queries LIMIT {limit};"
        else:
            sql_query = "SELECT id as query_id, query as text FROM queries;"
            
        self.queries_original = pd.read_sql_query(sql_query, self.dbconn)
        self.dbconn.close()
    
    def generate_batch_files(self, language='FR'):
        batch_size = self.config.openai.batch_size
        num_batches = (len(self.queries_original) // batch_size) + 1
        batch_files = []
        
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min(start_idx + batch_size, len(self.queries_original))
            batch_data = self.queries_original.iloc[start_idx:end_idx]

            expanded_queries = []
        
            for _, row in tqdm(batch_data.iterrows()):
            
                query_text = row['text']
                query_id = row['query_id']
                if language=='EN':
                    system_prompt = self.config.openai.system_prompt_EN
                elif language=="FR":
                    system_prompt = self.config.openai.system_prompt_FR
                else:
                    raise NotImplementedError('Only EN and FR supported as system prompt languages')
                expanded_query = {
                    "custom_id": f"query-{query_id}",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model":self.config.openai.model,
                        "temperature":self.config.openai.temperature,
                        "top_p":self.config.openai.top_p,
                        "frequency_penalty":self.config.openai.frequency_penalty,
                        "presence_penalty":self.config.openai.presence_penalty,
                        "max_tokens":self.config.openai.max_tokens,
                        "response_format":{ 
                                "type": "json_object"
                            },
                        "messages":[
                                {
                                    "role": "system",
                                    "content": system_prompt
                                },
                                {
                                    "role": "user",
                                    "content": query_text
                                }
                            ],
                        }
                }
                
                expanded_queries.append(expanded_query)
            
            
            batch_filename = f"{self.config.files.batch_filename}_batch_{i + 1}.jsonl"
            self.store_batch_file(batch_filename, expanded_queries)
            batch_files.append(batch_filename)
        return batch_files
    
    @staticmethod
    def store_batch_file(batch_filename, batch_queries):
        with open(batch_filename, 'w') as file:
            for obj in batch_queries:
                file.write(json.dumps(obj) + '\n')
    
    
    def upload_files(self, batch_files):
        """Upload each batch file separately."""
        batch_jobs = []

        status = 'failed'
        for batch_file in batch_files:
            while status=='failed':
                
                print(f"Uploading: {batch_file}")

                # Upload batch file
                uploaded_file = self.client.files.create(
                    file=open(batch_file, "rb"),
                    purpose="batch"
                )

                # Create batch job
                batch_job = self.client.batches.create(
                    input_file_id=uploaded_file.id,
                    endpoint="/v1/chat/completions",
                    completion_window="24h"
                )
                
                report = self.client.batches.retrieve(batch_job.id)
                
                status = report.status
                
                batch_jobs.append(batch_job)
                print(f"Batch job created: {batch_job.id}")
                time.sleep(5)

        return batch_jobs
  
    def generate_one_expansion(self, query):
        
        response = self.client.chat.completions.create(
            model = self.config.openai.model,
            temperature = self.config.openai.temperature,
            top_p = self.config.openai.top_p,
            frequency_penalty=self.config.openai.frequency_penalty,
            presence_penalty=self.config.openai.presence_penalty,
            max_tokens=self.config.openai.max_tokens,
            
            response_format={ 
                "type": "json_object"
            },
            
            messages=[
                {
                    "role": "system",
                    "content": self.config.openai.system_prompt_FR
                },
                {
                    "role": "user",
                    "content": query
                }
            ],
            )

        return response.choices[0].message.content

    def post_process_results(self, filename):
        with open(filename) as f:
            data = [json.loads(line) for line in f]
        
        ids = []
        results = []
        for output in data:
            id = output['custom_id'].split('-')[1]
            try:
                results.append(json.loads(output['response']['body']['choices'][0]['message']['content'])['results'])
                ids.append(id)
            except:
                print(f'Error in Json for query id={id}; Skipping..')

        out = pd.concat([pd.DataFrame(ids), pd.DataFrame(results)], axis=1).iloc[:,:6] #we allow max 5 query expansions
        
        out.columns = ['query_id', 'expansion1', 'expansion2', 'expansion3', 'expansion4', 'expansion5']
         
        return out
 
    def monitor_batch_jobs(self, batch_jobs, check_interval=30, download=True):
        all_completed = False

        while not all_completed:
            all_completed = True  
            print("Checking batch job statuses...")

            for batch_job in batch_jobs:
                batch_id = batch_job.id
                updated_batch = self.client.batches.retrieve(batch_id)

                completed = updated_batch.request_counts.completed
                failed = updated_batch.request_counts.failed
                total = updated_batch.request_counts.total
                status = updated_batch.status
                output_file_id = updated_batch.output_file_id 

                completion_rate = (completed / total) * 100 if total > 0 else 0
                failure_rate = (failed / total) * 100 if total > 0 else 0

                print(f"Batch {batch_id}**")
                print(f"Status: {status}")
                print(f"Completed: {completed}/{total} ({completion_rate:.2f}%)")
                print(f"Failed: {failed}/{total} ({failure_rate:.2f}%)\n")

                if status=='failed':
                    print('a')
                 
                if status not in ["completed", "failed"]:
                    all_completed = False
                
                if download:
                    if status == "completed" and output_file_id:
                        print(f" Output file available: {output_file_id}")
                        self.download_output_file(output_file_id, f"data/batch_results_{batch_id}.jsonl")
                

            if not all_completed:
                print(f"Waiting for {check_interval} seconds before checking again...\n")
                time.sleep(check_interval) 
                
            
        print("All batch jobs completed")
        
    def download_output_file(self, file_id, output_filename):
        """Downloads the batch output file and saves it locally."""
        output_file = self.client.files.retrieve_content(file_id)

        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(output_file)
        
        print(f"Results saved as: {output_filename}")
    
                 
if __name__ == "__main__":
    
    with open("config/openai_query_expansion.yml", "r") as file:
        config = ConfigBox(yaml.safe_load(file))
    
    expand_queries = False
    query_expansion = OpenAIQueryExpansion(config=config)
    query_expansion.load_original_queries(limit=None)
        
    if expand_queries:
 
        batch_files = query_expansion.generate_batch_files()
        batch_jobs = query_expansion.upload_files(batch_files)
        
        query_expansion.monitor_batch_jobs(batch_jobs)
    else:
        outputfiledir = 'data/query-expansions/output/'
        files = os.listdir(outputfiledir)
        
        individual_dfs = []
        for f in files:
            out = query_expansion.post_process_results(f"{outputfiledir}{f}")
            individual_dfs.append(out)
        all_expansions = pd.concat(individual_dfs)
        all_expansions['query_id'] = all_expansions['query_id'].astype(str)  # or astype(int) depending on what you want
        query_expansion.queries_original['query_id'] = query_expansion.queries_original['query_id'].astype(str)

        expanded_queries_full = pd.merge(query_expansion.queries_original, all_expansions, how='left', left_on='query_id', right_on='query_id')
        expanded_queries_full.to_csv('data/query-expansions/expanded_results.csv')
   
        
        
   
    
        
    