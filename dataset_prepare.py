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


""" Похожие вопросы: """
fa_ids = sys_df["TemplateID"].unique()

similar_queries = []
dissimilar_queries = []
for fa_id in fa_ids:
    query_tuples = list(sys_df[sys_df["TemplateID"] == fa_id].itertuples(index=False))
    temp_sim_queries = [(tpl1.TemplateID, tpl2.TemplateID, tpl1.LmQuery, tpl1.Query, tpl2.LmQuery, tpl2.Query) for tpl1 in query_tuples for tpl2 in query_tuples if 
                        tpl1.LmQuery != tpl2.LmQuery]
    similar_queries += temp_sim_queries
    


print(similar_queries[:5])
print(len(similar_queries))

""" Непохожие вопросы: """
dissimilar_queries = []
data_tuples = list(sys_df.itertuples(index=False))
dissimilar_queries =  [(tpl1.TemplateID, tpl1.LmQuery, tpl1.Query, tpl2.TemplateID, tpl2.LmQuery, tpl2.Query) for tpl1 in data_tuples for 
                       tpl2 in data_tuples if tpl1.TemplateID != tpl2.TemplateID]


print(similar_queries[:10])
print(len(similar_queries))
