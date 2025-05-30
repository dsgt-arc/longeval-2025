import asyncio

import pandas as pd
from rerankers import Reranker
from tqdm.asyncio import tqdm_asyncio


class Rerank:
    def __init__(self, config):
        self.max_concurrency = config.parallel.max_concurrency

        self.ranker = Reranker(
            model_name=config.reranking["model_name"],
            model_type=config.reranking["model_type"],
            lang=config.reranking["lang"],
            batch_size=config.reranking["batch_size"],
            device="cuda",
            verbose=1,
            trust_remote_code=True,
        )

        print(
            f"Reranker initialised: {config.reranking['model_name']} "
            f"(type={config.reranking['model_type']}, "
            f"batch_size={config.reranking['batch_size']}, "
            f"max_concurrency={self.max_concurrency})"
        )

    def rerank_stage(self, data: pd.DataFrame) -> pd.DataFrame:
        print("Starting reranking")
        return asyncio.run(self._rerank_dataframe_async(data))

    async def _rerank_dataframe_async(self, df):
        sem = asyncio.Semaphore(self.max_concurrency)
        tasks = []

        for qid, grp in df.groupby("qid"):
            query_text = grp["query"].iloc[0]
            docs = grp["contents"].tolist()
            doc_ids = grp["docid"].tolist()

            async def bounded_rerank(qid=qid, q=query_text, d=docs, ids=doc_ids):
                async with sem:
                    return await self._rerank_one_query(qid, q, d, ids)

            tasks.append(bounded_rerank())

        results = await tqdm_asyncio.gather(
            *tasks, total=len(tasks), desc="Reranking queries"
        )
        flat_rows = [row for per_query in results for row in per_query]
        return pd.DataFrame(flat_rows)

    async def _rerank_one_query(self, qid, query_text, docs, doc_ids):
        ranked = await self.ranker.rank_async(
            query=query_text, docs=docs, doc_ids=doc_ids
        )

        return [
            {
                "qid": qid,
                "docid": res.doc_id,
                "rerank_score": res.score,
                "rerank_rank": res.rank,
            }
            for res in ranked
        ]
