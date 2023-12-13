import os
import pandas as pd

sim_queries_df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_similar_queries.csv"), sep="\t")
answers_df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_answers.csv"), sep="\t")

print(sim_queries_df.info())
print(answers_df.info())

sim_answers_queries_df = pd.merge(sim_queries_df, answers_df, left_on=["sys", "id"], right_on=["sys_id", "ID"])
print(sim_answers_queries_df)

sim_answers_queries_df.to_csv(os.path.join(os.getcwd(), "data", "231207", "sim_answers_queries.tsv"), sep="\t", index=False)
