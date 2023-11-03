import os
import pandas as pd
from src.start import (data_upload,
                        queries_analysis)
from src.config import logger

sys_ids = [1, 14, 15, 2, 10, 8, 3]

for sys_id in sys_ids:
    data_upload.sys_data_upload(sys_id)
    clusters_dicts = data_upload.get_clusters()
    answers_dicts = data_upload.get_answers()
    answers_df = pd.DataFrame(answers_dicts)
    
    """запишем ответы:"""
    answers_df.to_csv(os.path.join(os.getcwd(), "data", "dataset_answers.csv"), sep="\t", index_label=False, index=False, mode="a")
    
    
    """
    Оставим только непохожие вопросы, в количестве
    не более 10 для каждого быстрого ответа
    """
    queries_for_train = []
    clusters_df = pd.DataFrame(clusters_dicts)
    fa_ids = set(list(clusters_df["id"]))
    for num, fa_id in enumerate(fa_ids):
        logger.info(str("sys:" + str(sys_id) + " " + str(num) + "/" + str(len(fa_ids))))
        quries = list(clusters_df["query"][clusters_df["id"] == fa_id])
        if len(quries) <= 300:
            queries_for_train += [{"sys": sys_id, "id": fa_id, "query": q.query, "lem_query": q.lem_query, } for q in queries_analysis(10, 0.9, quries)]

    queries_for_train_df = pd.DataFrame(queries_for_train)
    queries_for_train_df.to_csv(os.path.join(os.getcwd(), "data", "dataset_queries.csv"), sep="\t", index_label=False, index=False, mode="a")