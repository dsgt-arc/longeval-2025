import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import dotenv
import requests
import tqdm
from pyspark.sql import functions as F

from longeval.collection import ParquetCollection
from longeval.spark import get_spark

dotenv.load_dotenv(Path("~/clef/longeval-2025/.env").expanduser().as_posix())


def get_schema():
    return {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "qid": {
                    "type": "string",
                    "description": "The unique identifier for the query.",
                },
                "query": {
                    "type": "string",
                    "description": "The text of the query.",
                },
            },
            "required": ["qid", "query"],
            "additionalProperties": False,
        },
    }


def prompt(queries):
    query_text = "\n".join([f"{row['qid']}: {row['query']}" for row in queries])
    return f"""{query_text}

    For each query above, generate a query expansion in French that includes additional relevant terms or phrases.
    The query expansion should be no more than 100 words long.
    The query engine relies on BM25 and vector search techniques in French.
    The output should be a JSON array of objects, each containing the original 'qid' and the expanded 'query'.
    """


def chat_complete(
    queries,
    api_key=os.environ.get("OPENROUTER_API_KEY"),
    model="deepseek/deepseek-chat-v3-0324",
    # model="google/gemini-2.5-flash-preview-05-20",
    # model="google/gemma-3n-e4b-it:free",
):
    completion = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=dict(
            model=model,
            messages=[
                {"role": "user", "content": [{"type": "text", "text": prompt(queries)}]}
            ],
            # object with predict list of strings and a reason string
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "longeval",
                    "strict": True,
                    "schema": get_schema(),
                },
            },
        ),
    )
    return completion.json()


def query_expansion(queries, output):
    # add the response to a logfile
    output = Path(output).expanduser()
    logfile = Path(output) / "completion/log.txt"
    # name is start-end range
    start = queries[0]["qid"]
    end = queries[-1]["qid"]
    output = Path(output) / f"expansion/{start}-{end}.json"
    if output.exists():
        # print(f"Output file {output} already exists, skipping.")
        return
    logfile.parent.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)

    resp = chat_complete(queries)
    with logfile.open("a") as f:
        f.write(json.dumps(resp) + "\n")

    msg = resp["choices"][0]["message"]["content"]
    try:
        data = json.loads(msg)
    except Exception as e:
        raise ValueError(
            f"Failed to parse JSON response: {msg}\nError: {e}\nResponse: {resp}"
        )
    # check that all of the quids are present
    input_qids = {q["qid"] for q in queries}
    output_qids = {d["qid"] for d in data}
    if input_qids != output_qids:
        raise ValueError(
            f"Input and output qids do not match. {input_qids - output_qids}", data
        )

    # write the output to a file
    with output.open("w") as f:
        json.dump(data, f, indent=2)

    # return the data
    return data


def main():
    spark = get_spark(cores=8, memory="20g")

    root = Path("~/shared/longeval/2025/parquet").expanduser()
    train = ParquetCollection(spark, (root / "train").as_posix())
    test = ParquetCollection(spark, (root / "test").as_posix())
    queries = train.queries.union(test.queries).cache()
    queries.printSchema()

    deduped = (
        queries.groupBy("qid")
        .agg(F.first("query").alias("query"))
        .orderBy(F.col("qid").cast("integer"))
        .cache()
    )

    batch_size = 100
    deduped_rows = deduped.collect()
    batches = [
        deduped_rows[i : i + batch_size]
        for i in range(0, len(deduped_rows), batch_size)
    ]

    def process_batch(batch, idx):
        try:
            query_expansion(batch, "~/scratch/longeval/query_expansion")
        except Exception as e:
            print(batch)
            print(
                f"Error processing batch {idx * batch_size}-{(idx + 1) * batch_size}: {e}"
            )

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(process_batch, batch, idx)
            for idx, batch in enumerate(batches)
        ]
        for _ in tqdm.tqdm(as_completed(futures), total=len(futures)):
            pass


if __name__ == "__main__":
    main()
