"""
как извлечь все документы из индекса: 
https://discuss.elastic.co/t/get-all-documents-from-an-index/86977/5
работающий скрипт, обращаюйщийся в эластик для извлечения документов
"""

import os
import asyncio
import pandas as pd
from src.storage import ElasticClient

es = ElasticClient()
# es_index = "expert_bot_results"
es_index = "clusters"
search_func = es.search(
        allow_partial_search_results=True,
        min_score=0,
        index=es_index,
        query={"match_all" : {}},
        scroll='1m',
        size=5000)

loop = asyncio.get_event_loop()
res = loop.run_until_complete(search_func)




k = 1
res_hits = ["a"]
results_dfs = []
if es_index == "expert_bot_results":
    while len(res_hits)>0:
    # while len(results_dfs) <= 5:
        scroll = res['_scroll_id']
        func2 = es.scroll(scroll_id = scroll, scroll = '1m')
        res = loop.run_until_complete(func2)
        res_hits = res["hits"]["hits"]
        res_hits_df = pd.DataFrame([{**{"bot": d["_source"]["bot"]},
                                     **d["_source"]["request"],
                                     **d["_source"]["response"],
                                     **{"timestamp": d["_source"]["timestamp"]}} for d in res_hits])
        results_dfs.append(res_hits_df)
        print(len(results_dfs), len(res_hits), len(results_dfs) * len(res_hits))
        k += 1

else:
    while len(res_hits)>0:
        scroll = res['_scroll_id']
        func2 = es.scroll(scroll_id = scroll, scroll = '1m')
        res = loop.run_until_complete(func2)
        res_hits = res["hits"]["hits"]
        res_hits_df = pd.DataFrame([d["_source"] for d in res_hits])
        results_dfs.append(res_hits_df)
        print(len(results_dfs), len(res_hits), len(results_dfs) * len(res_hits))
        k += 1

loop.close()
results_df = pd.concat(results_dfs, axis=0)
print(results_df)


results_df.to_csv(os.path.join("data", "240312", "clusters.tsv"), sep="\t", index=False)
results_df.to_feather(os.path.join("data", "240312", "clusters.feather"))

# results_df.to_csv(os.path.join("data", "240312", "es_queries_answers.tsv"), sep="\t", index=False)
# results_df.to_feather(os.path.join("data", "240312", "es_queries_answers.feather"))
