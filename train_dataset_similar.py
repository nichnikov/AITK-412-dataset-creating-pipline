import os
import pandas as pd

df = pd.read_csv(os.path.join(os.getcwd(), "data", "dataset_queries.csv"), sep="\t")
print(df)

ids = list(set(df["id"]))
print(len(ids))

similar_queries = []
for num, id in enumerate(ids):
    temp_queries = list(df["lem_query"][df["id"] == id])
    similar_queries += [{"query1": x, "query2": y} for x, y in [tuple(sorted([q1, q2])) for q1 in temp_queries for q2 in temp_queries if q1 != q2]]
    print(num, "/", len(ids))


similar_queries_df = pd.DataFrame(similar_queries)
print(similar_queries_df.shape)
similar_queries_df.drop_duplicates(inplace=True)
print(similar_queries_df.shape)
similar_queries_df.to_csv(os.path.join(os.getcwd(), "data", "dataset_similar_queries.csv"), sep="\t", index=False)