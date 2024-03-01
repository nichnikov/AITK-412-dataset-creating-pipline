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

QueriesTuple = namedtuple("QueriesTuple", "lm_query, query")

sys_df = pd.read_csv(os.path.join("datasets", "sys_1_thinned_questions.csv"), sep="\t")
print(sys_df)

fa_ids = sys_df["TemplateID"].unique()

similar_queries = []
for fa_id in fa_ids:
    query_tuples = [QueriesTuple(*x) for x in list(sys_df[sys_df["TemplateID"] == fa_id][["LmQuery", "Query"]].itertuples(index=False, name=None))]
    temp_sim_queries = [(tpl1.lm_query, tpl1.query, tpl2.lm_query, tpl2.query) for tpl1 in query_tuples for tpl2 in query_tuples if 
                        tpl1.lm_query != tpl2.lm_query]
    similar_queries += temp_sim_queries

print(similar_queries[:10])
print(len(similar_queries))

