"""
Очищенные вопросы разбиваются на пары похожих и непохожих, 
массивы пар похожих и непохожих вопросов разбиваются на 
- train
- val
- test
"""

import os
import pandas as pd
from collections import namedtuple
from src.config import logger

QueriesTuple = namedtuple("QueriesTuple", "lm_query, query")
# [1, 2, 3, 4, 8, 10, 11, 13, 14, 15, 16, 21, 22, 27, 28, 34, 37, 45, 47, 50, 51, 54, 55]

for sys_id in [1, 2, 3, 4, 8, 10, 11, 13, 14, 15, 16, 21, 22, 27, 28, 34, 37, 45, 47, 50, 51, 54, 55]:
    '''
    Прореженные вопросы с удаленными дублями:
    После выполнения обработок:
    - thinned_queries.py
    - duplicates_remove.py
    
    '''

    # in_fn = "_".join(["sys", str(sys), "thinned_questions.csv"])
    # sys_df = pd.read_csv(os.path.join("datasets", in_fn), sep="\t")
    in_fn = "_".join(["sys", str(sys_id), "thinned_questions_without_duplicates.feather"])
    sys_df = pd.read_feather(os.path.join(os.getcwd(), "clusters",  in_fn))

    """ Похожие вопросы: """
    fa_ids = sys_df["TemplateID"].unique()

    similar_queries = []
    dissimilar_queries = []
    for fa_id in fa_ids:
        query_tuples = list(sys_df[sys_df["TemplateID"] == fa_id].itertuples(index=False))
        temp_sim_queries = [{"TemplateID": tpl1.TemplateID, "LmQuery1": tpl1.LmQuery, "Query1": tpl1.Query, 
                            "LmQuery2": tpl2.LmQuery, "Query2": tpl2.Query} for tpl1 in query_tuples for tpl2 in query_tuples if 
                            tpl1.LmQuery != tpl2.LmQuery]
        similar_queries += temp_sim_queries  


    similar_queries_df = pd.DataFrame(similar_queries)
    logger.info("Make {} pairs of similar queries".format(str(len(similar_queries))) )
    out_fn = "".join(["similar_queries_sys_", str(sys_id), ".feather"])
    similar_queries_df.to_feather(os.path.join("datasets", out_fn))
