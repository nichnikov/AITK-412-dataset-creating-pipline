import os
import pandas as pd

sys_pubs_df = pd.read_csv(os.path.join("base_data", "sys_pub_mappings.csv"), sep="\t")

# es_queries_df = pd.read_csv(os.path.join("data", "240314", "es_queries_answers_etalons_without_greetings.csv"), sep="\t")
# es_queries_df.to_feather(os.path.join("datasets", "es_queries", "es_queries_answers_etalons_without_greetings.feather"))
es_queries_df = pd.read_feather(os.path.join("datasets", "es_queries", "es_queries_answers_etalons_without_greetings.feather"))

main_systems = [1, 2, 3, 15, 8]

for sys_id in main_systems:
    sys_queries_df = es_queries_df[(es_queries_df["sys"] == sys_id)&(es_queries_df["algorithm"].isin(["Sbert", "SbertT5"]))]
    sys_queries_df["query_len"] = sys_queries_df["text"].apply(lambda tx: len(tx.split()))
    sys_queries_df_ = sys_queries_df[sys_queries_df["query_len"] >= 5]
    print(sys_id, sys_queries_df_.shape)
    sys_queries_df_.drop(["algorithm", "score", "query_len", "userid", "chat_id"], axis=1).to_csv(os.path.join("datasets", "es_queries", "sys_" + str(sys_id) + "_es_queries.csv"), sep="\t", index=False)
