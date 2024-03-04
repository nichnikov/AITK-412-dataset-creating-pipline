import os
import pandas as pd
from collections import namedtuple

Queries = namedtuple("Queries", "lm_query, query")

df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_queries.csv"), sep="\t")
print(df)

sys_ids = list(set(df["sys"]))

similar_lm_queries = []
for sys_id in sys_ids:
    ids = list(set(df[df["sys"] == sys_id]["id"]))

    for num, id in enumerate(ids):
        temp_queries = list(df["query"][df["id"] == id])
        temp_lm_queries = list(df["lem_query"][df["id"] == id])
        
        similar_lm_queries += [{"sys": sys_id, 
                                "id": id, 
                                "query1": x.query, 
                                "query2": y.query, 
                                "lm_query1": x.lm_query, 
                                "lm_query2": y.lm_query} for x, y in 
                            [tuple(sorted([Queries(lm_q1, q1), Queries(lm_q2, q2)])) for lm_q1, q1 in 
                             zip(temp_lm_queries, temp_queries) for lm_q2, q2 in 
                             zip(temp_lm_queries, temp_queries) 
                             if lm_q1 != lm_q2]]
        print(sys_id, num, "/", len(ids))


similar_queries_df = pd.DataFrame(similar_lm_queries)
print(similar_queries_df.shape)
similar_queries_df.drop_duplicates(inplace=True)
print(similar_queries_df.shape)
similar_queries_df.to_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_similar_queries.csv"), sep="\t", index=False)