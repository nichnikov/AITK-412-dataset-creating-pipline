"""
Формирование датасета непохожих вопросов
каждый вопрос отправляется в Эластик, к нему находятся вопросы из индекса "thinned_fa_questions"
в качестве "непохожих" признаются вопросы, имеющие различные TemplateID
"""

import os
import asyncio
import pandas as pd
from src.config import logger
from src.storage import ElasticClient


es = ElasticClient()
index = "thinned_fa_questions"
loop = asyncio.get_event_loop()

# [1, 2, 3, 4, 8, 10, 11, 13, 14, 15, 16, 21, 22, 27, 28, 34, 37, 45, 47, 50, 51, 54, 55]
for sys_id in [3, 4, 8, 10, 11, 13, 14, 15, 16, 21, 22, 27, 28, 34, 37, 45, 47, 50, 51, 54, 55]:
    in_fn = "_".join(["sys", str(sys_id), "thinned_questions.csv"])
    sys_df = pd.read_csv(os.path.join(os.getcwd(), "datasets",  in_fn), sep="\t")

    sys_df.dropna(inplace=True)
    sys_queries_dcts = sys_df.to_dict(orient="records")
    
    
    try:
        loop.run_until_complete(es.delete_index(index))
    except:
        logger.exception("There no index {}".format(index))
    
    try:
        loop.run_until_complete(es.create_index(index))
    except:
        loop.close()

    try:
        loop.run_until_complete(es.add_docs(index, sys_queries_dcts))
    except:
        loop.close()
  
    dissimilar_queries = []
    for num, query_dict in enumerate(sys_queries_dcts):
        logger.info(str(num + 1) + "/" + str(len(sys_queries_dcts)))
        query_dct={"match": {"LmQuery": query_dict["LmQuery"]}}
        try:
            search_result_dct = loop.run_until_complete(es.search_by_query(index, query_dct))
            dissimilar_queries += [(*tuple(query_dict.values()), *tuple(d["_source"].values())) for d in search_result_dct["hits"]["hits"][:25] if 
                                d["_source"]["TemplateID"] != query_dict["TemplateID"]]
        except:
            logger.exception("There is problem with seaching text: ".format(str(query_dict["LmQuery"])))

    dissimilar_queries_df = pd.DataFrame(dissimilar_queries, columns = ["TemplateID1", "LmQuery1", "Query1", "TemplateID2", "LmQuery2", "Query2"])

    out_fn = "".join(["dissimilar_queries_sys_", str(sys_id), ".feather"])
    dissimilar_queries_df.to_feather(os.path.join("datasets", out_fn))

loop.close()