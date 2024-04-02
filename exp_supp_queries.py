"""Подготовка тестовых выборок: Вопросы ЭП - эталоны Быстрых ответов для валидации"""
import os
import pandas as pd

# es_qrs_anws_df = pd.read_csv(os.path.join("data", "240311", "es_queries_answers.csv"), sep="\t")


df = pd.read_feather(os.path.join("data", "240311", "es_queries_answers.feather"))
print(df)
print(df.info())

bss_pubs = [6, 8, 9, 186, 188, 220]
del_template_id = [111100001, 100000077, 100000013]
bss_queries_df = df[(df["pubid"].isin(bss_pubs)) & (~df["algorithm"].isin(["Jaccard"])) 
                    & (~df["templateId"].isin(del_template_id)) & (df["bot"] == "kuber")]
bss_queries_df.drop_duplicates(inplace=True)
bss_queries_df.to_csv(os.path.join("data", "bss_queries_answers.csv"), sep="\t", index=False)