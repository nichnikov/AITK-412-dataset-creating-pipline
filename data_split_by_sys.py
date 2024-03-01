import os
import pandas as pd

all_df = pd.read_csv(os.path.join("data", "240228", "all_clusters.tsv"), sep="\t")
print(all_df)

for sys in all_df["SysID"].unique():
    sys_df = all_df[all_df["SysID"] == sys]
    print(sys_df)
    fn = "_".join(["sys", str(sys), "clusters.tsv"])
    sys_df.to_csv(os.path.join("data", "240228", fn), sep="\t", index=False)