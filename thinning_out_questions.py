import os
import pandas as pd

df = pd.read_feather(os.path.join("data", "240212", "all_clusters.feather"))

print(df)