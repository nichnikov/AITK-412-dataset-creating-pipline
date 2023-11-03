"""Удаление непохожих вопросов с похожими ответатми на основании оценки похожести ответов"""

import os
import pandas as pd

not_sim_queries_df = pd.read_csv(os.path.join(os.getcwd(), "data", "queries_dataset_0.tsv"), sep="\t")
print(not_sim_queries_df)
queries_with_answers_sim_df = pd.read_csv(os.path.join(os.getcwd(), "data", "queries_with_answers_sim.tsv"), sep="\t")
print(queries_with_answers_sim_df)
sims_answs_df = queries_with_answers_sim_df[queries_with_answers_sim_df["cos_sims"] >= 0.9]
print(sims_answs_df)
# print(len(list(sims_answs_df[["answ_id1", "answ_id2"]].itertuples(index=False, name=None))))

for id1, id2 in sims_answs_df[["answ_id1", "answ_id2"]].itertuples(index=False, name=None):
    del_rows_df = not_sim_queries_df[(not_sim_queries_df["answ_id1"] == str(id1)) & (not_sim_queries_df["answ_id2"] == str(id2))]
    not_sim_queries_df = not_sim_queries_df.drop(del_rows_df.index)
    
print(not_sim_queries_df)
not_sim_queries_df.to_csv(os.path.join(os.getcwd(), "data", "dataset_without_sim_answers0.tsv"), sep="\t", index=False)