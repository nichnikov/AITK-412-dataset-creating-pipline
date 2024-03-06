import os
import pandas as pd
'''
for sys_id in [1, 2, 3, 4, 8, 10, 11, 13, 14, 15, 16, 21, 22, 27, 28, 34, 37, 45, 47, 50, 51, 54, 55]:
    sim_fn = "".join(["similar_queries_sys_", str(sys_id), ".feather"])
    dissim_fn = "".join(["dissimilar_queries_sys_", str(sys_id), ".feather"]) 
    queries_fn = "_".join(["sys", str(sys_id), "clusters.feather"])
    queries_fn = "_".join(["sys", str(sys_id), "clusters.feather"])
    thinned_queries_fn = "_".join(["sys", str(sys_id), "thinned_questions.feather"])
    
    sim_df = pd.read_feather(os.path.join("datasets", sim_fn))
    dissim_df = pd.read_feather(os.path.join("datasets", dissim_fn))
    queries_df = pd.read_feather(os.path.join("clusters", queries_fn))
    thinned_queries_df = pd.read_feather(os.path.join("clusters", thinned_queries_fn))
    print("sys:", sys_id, "queries quantity:", queries_df.shape[0], "thinned queries quantity:", thinned_queries_df.shape[0], 
          "sim quantity:", sim_df.shape[0], "dissim quantity:", dissim_df.shape[0])
'''    
'''
    # запись в clusters прореженных вопросов в формате feather
    thinned_queries_fn = "_".join(["sys", str(sys_id), "thinned_questions.csv"])
    thinned_queries_df = pd.read_csv(os.path.join("datasets", thinned_queries_fn), sep="\t")
    thinned_queries_feather_fn = "_".join(["sys", str(sys_id), "thinned_questions.feather"])
    thinned_queries_df.to_feather(os.path.join("clusters", thinned_queries_feather_fn))
    '''
    

df = pd.read_feather(os.path.join("clusters", "sys_1_thinned_questions.feather"))
print(df)
df.to_csv(os.path.join("clusters", "sys_1_thinned_questions.csv"), sep="\t", index=False)