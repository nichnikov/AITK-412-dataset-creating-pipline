import os
import pandas as pd

dissim_queries_df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_dissimilar_queries.csv"), sep="\t")


sys_ids = set(list(dissim_queries_df["sys"]))


for sys_id in sys_ids:
    id1_id2 = [tuple(sorted(tpl)) for tpl in dissim_queries_df[["id1", "id2"]][dissim_queries_df["sys"] == sys_id].itertuples(index=False)]
    
    
    print(sys_id, id1_id2[:10])
    print(sys_id, len(id1_id2))
    print(sys_id, len(set(id1_id2)))
