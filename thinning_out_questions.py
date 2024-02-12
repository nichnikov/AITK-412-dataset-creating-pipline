import os
import pandas as pd
from src.config import logger
from src.start import queries_analysis

clusters_df = pd.read_feather(os.path.join("data", "240212", "all_clusters.feather"))
print(clusters_df)
clusters_df = clusters_df[:10000]
sys_ids = clusters_df["SysID"].unique()
queries_dicts = []

for sys_id in sys_ids:
    fa_ids = clusters_df["ID"].unique()
    for num, fa_id in enumerate(fa_ids):
        logger.info(str("sys:" + str(sys_id) + " " + str(num) + "/" + str(len(fa_ids))))
        quries = list(clusters_df["query"][clusters_df["id"] == fa_id])
        if len(quries) <= 300:
            queries_dicts += [{"sys": sys_id, "id": fa_id, "query": q.query, "lem_query": q.lem_query, } for q in queries_analysis(10, 0.9, quries)]
