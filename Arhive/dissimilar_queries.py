import os
import asyncio
import pandas as pd
from src.storage import ElasticClient

queries_df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_queries.csv"), sep="\t")
print(queries_df)

queries_dicts = queries_df.to_dict(orient="records")
print(queries_dicts[:5])

es = ElasticClient()
loop = asyncio.get_event_loop()
not_similar_queries = []
for num, d in enumerate(queries_dicts):
    print(num, "/", len(queries_dicts))
    try:
        query={"bool": {"must": [
                                {"match_phrase": {"sys": d["sys"]}},
                                                {"match": {"lem_query": d["lem_query"]}}]}}
        res = loop.run_until_complete(es.search_by_query("dataset_queries", query))
        if res["hits"]["hits"]:
            temp_pairs = []
            for rd in res["hits"]["hits"]:
                if int(d["id"]) != int(rd["_source"]["id"]):
                    temp_pairs += [{"sys": d["sys"], "id1": x[0], "id2": y[0], "lem_query1": x[1], "lem_query2": y[1]} 
                                    for x, y in [tuple(sorted([(str(d["id"]), d["lem_query"]), (str(rd["_source"]["id"]), rd["_source"]["lem_query"])]))]]
            not_similar_queries += temp_pairs[:10]
    except:
        pass

loop.close()

not_similar_queries_df = pd.DataFrame(not_similar_queries)
print(not_similar_queries_df.shape)
not_similar_queries_df.drop_duplicates(inplace=True)
print(not_similar_queries_df.shape)
not_similar_queries_df.to_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_dissimilar_queries.csv"), sep="\t", index=False)