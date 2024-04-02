import os
import pandas as pd

df = pd.read_csv(os.path.join("data", "bss_queries_answers.csv"), sep="\t")
print(df)

'''
df["text_len"] = df["text"].apply(lambda x: len(x.split()))
df.to_csv(os.path.join("data", "bss_queries_answers.csv"), sep="\t", index=False)'''