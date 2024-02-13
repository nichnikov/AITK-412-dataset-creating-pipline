import os
import pandas as pd

df = pd.read_feather(os.path.join("data", "240212", "thinning_clusters.feather"))
print(df)

df_unique = df[["sys", "id", "query", "lem_query"]].drop_duplicates()
print(df_unique)
df_unique.to_feather(os.path.join("data", "240212", "thinning_clusters_unique.feather"))
df_unique.to_csv(os.path.join("data", "240212", "thinning_clusters_unique.csv"), sep="\t", index=False)