import os
import pandas as pd

dissimilar_queries_df = pd.read_feather(os.path.join("datasets", "dissimilar_queries_sys_2.feather"))
print(dissimilar_queries_df)
dissimilar_queries_df.to_csv(os.path.join("datasets", "dissimilar_queries_sys_2.csv"), sep="\t", index=False)
