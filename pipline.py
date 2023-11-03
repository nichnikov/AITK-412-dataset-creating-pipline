import os
import pandas as pd
from src.start import (data_upload,
                       queries_analysis,
                       tokenizer)
from src.config import logger
import asyncio
from src.storage import ElasticClient


es = ElasticClient()

queries_index = "dataset_queries"
answers_index = "dataset_answers"

for index in [queries_index, answers_index]:
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(es.delete_index(index))
        loop.run_until_complete(es.create_index(index))
        loop.close()
    except:
        loop.close()
        logger.info("There no index {}".format(index))

sys_ids = [1, 14, 15, 2, 10, 8, 3]

for sys_id in sys_ids:
    data_upload.sys_data_upload(sys_id)
    logger.info("data uploading for sys ID {}".format(str(sys_id)))

    answers_dicts = data_upload.get_answers()
    lem_answers = [" ".join(tk_a) for tk_a in tokenizer([d["ShortAnswer"] for d in answers_dicts])]
    logger.info("answers lematized for sys ID {}".format(str(sys_id)))

    """добавим лемматизированные ответы и запишем в файд:"""
    answers_dicts = [{**d, **{"LemAnswer": la}} for la, d in zip(lem_answers, answers_dicts)]
    answers_df = pd.DataFrame(answers_dicts)
    
    
    answers_df.to_csv(os.path.join(os.getcwd(), "data", "dataset_answers.csv"), sep="\t", index_label=False, index=False, mode="a")
    logger.info("Dataframe with answers for sys ID {} saved".format(str(sys_id)))
    
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(es.add_docs(answers_index, answers_dicts))
        loop.close()
    except:
        loop.close()
        logger.info("There is problem with sending answers to elastic for Sys {}".format(str(sys_id)))
    
    logger.info("Answers for sys ID {} send to Elastic".format(str(sys_id)))
    
    """
    Оставим только непохожие вопросы, в количестве
    не более 10 для каждого быстрого ответа
    """
    clusters_dicts = data_upload.get_clusters()
    queries_dicts = []
    clusters_df = pd.DataFrame(clusters_dicts)
    fa_ids = set(list(clusters_df["id"]))
    for num, fa_id in enumerate(fa_ids):
        logger.info(str("sys:" + str(sys_id) + " " + str(num) + "/" + str(len(fa_ids))))
        quries = list(clusters_df["query"][clusters_df["id"] == fa_id])
        if len(quries) <= 300:
            queries_dicts += [{"sys": sys_id, "id": fa_id, "query": q.query, "lem_query": q.lem_query, } for q in queries_analysis(10, 0.9, quries)]

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(es.add_docs(queries_index, queries_dicts))
        loop.close()
    except:
        loop.close()
        logger.info("There is problem with sending queries to elastic for Sys {}".format(str(sys_id)))
    
    logger.info("Queries for sys ID {} send to Elastic".format(str(sys_id)))
    
    queries_for_train_df = pd.DataFrame(queries_dicts)
    queries_for_train_df.to_csv(os.path.join(os.getcwd(), "data", "dataset_queries.csv"), sep="\t", index_label=False, index=False, mode="a")