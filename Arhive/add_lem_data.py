import os
import pandas as pd
from src.start import tokenizer

df = pd.read_csv(os.path.join("data", "validate_queries_41week_mouses_compare.csv"), sep="\t")
# "FastAnswerText" -> "LemAnswer"
# "Query" -> "lm_query"
lm_query = [" ".join(lm) for lm in tokenizer(df["Query"].to_list())]
lm_query_df = pd.DataFrame(lm_query, columns=["lm_query"])

LemAnswer = [" ".join(lm) for lm in tokenizer(df["FastAnswerText"].to_list())]
LemAnswer_df = pd.DataFrame(LemAnswer, columns=["LemAnswer"])

df_lm = pd.concat([df, lm_query_df, LemAnswer_df], axis=1)

print(df_lm)
df_lm.to_csv(os.path.join("data", "validate_queries_41week_mouses_compare_lm.csv"), sep="\t", index=False)

