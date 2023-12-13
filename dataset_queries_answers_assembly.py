import os
import pandas as pd

sim_qrs_ans_df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "sim_answers_queries.tsv"), sep="\t")
dissim_qrs_ans_df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "dissim_queries_answers.tsv"), sep="\t")

print(sim_qrs_ans_df.info())
print(dissim_qrs_ans_df.info())

sim_qrs_ans_df["label"] = "Правда"
dissim_qrs_ans_df["label"] = "Ложь"

sim_qrs_ans_df_lem1 = sim_qrs_ans_df[["sys", "id", "lm_query1", "LemAnswer", "label"]].rename(columns={"lm_query1": "lm_query"})
sim_qrs_ans_df_lem2 = sim_qrs_ans_df[["sys", "id", "lm_query2", "LemAnswer", "label"]].rename(columns={"lm_query2": "lm_query"})
dissim_qrs_ans_df_lem = dissim_qrs_ans_df[["sys_id", "ID", "lem_query", "LemAnswer", "label"]].rename(columns={"sys_id": "sys", 
                                                                                                               "ID": "id",
                                                                                                               "lem_query":"lm_query"})
qwrs_answs_lem_df = pd.concat([sim_qrs_ans_df_lem1, sim_qrs_ans_df_lem2, dissim_qrs_ans_df_lem], axis=0)

sim_qrs_ans_df1 = sim_qrs_ans_df[["sys", "query1", "ShortAnswer", "label"]].rename(columns={"query1": "query"})
sim_qrs_ans_df2 = sim_qrs_ans_df[["sys", "query2", "ShortAnswer", "label"]].rename(columns={"query2": "query"})
dissim_qrs_ans_not_lm_df = dissim_qrs_ans_df[["sys_id", "query", "ShortAnswer", "label"]].rename(columns={"sys_id": "sys"})

qwrs_answs_not_lm_df = pd.concat([sim_qrs_ans_df1, sim_qrs_ans_df2, dissim_qrs_ans_not_lm_df], axis=0)


"""разбиение на тестовую и обучающую выборку и сохранение:"""
test_num = 5000
for df, nm in [(qwrs_answs_lem_df, "queries_answers_lem"), (qwrs_answs_not_lm_df, "queries_answers_not_lem")]:
    df_shuffle = df.sample(frac=1)
    for df_, sgn in [(df_shuffle[test_num:], "train"), (df_shuffle[:test_num], "test")]:
        df_.to_csv(os.path.join(os.getcwd(), "data", "231207", "dataset", "_".join([str(nm), str(sgn)]) + ".tsv"), 
                                 sep="\t", index=False)
         
'''
qwrs_answs_lem_df = qwrs_answs_lem_df.sample(frac=1)
qwrs_answs_not_lm_df = qwrs_answs_lem_df.sample(frac=1)


print(qwrs_answs_lem_df)
print(qwrs_answs_not_lm_df)

qwrs_answs_lem_df.to_csv(os.path.join(os.getcwd(), 
                                              "data", "231207", "dataset", "queries_answers_lem.tsv"), 
                                 sep="\t", index=False)
qwrs_answs_not_lm_df.to_csv(os.path.join(os.getcwd(), 
                                                 "data", "231207", "dataset", "queries_answers_not_lem.tsv"), 
                                    sep="\t", index=False)'''