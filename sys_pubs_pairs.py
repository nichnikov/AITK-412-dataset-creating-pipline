"""Соответствие систем и пабайди:"""

import os
import json
import pandas as pd

with open(os.path.join("base_data", "sys_pub_mappings.json")) as f:
    pabs_sys_dict = json.load(f)

sys_pubs = []
for i in pabs_sys_dict:
    sys_pubs += [(i, pb) for pb in pabs_sys_dict[i]]
    
sys_pubs_df = pd.DataFrame(sys_pubs, columns=["sys", "pub"])
sys_pubs_df.to_csv(os.path.join("base_data", "sys_pub_mappings.csv"), sep="\t", index=False)


'''
"""Код работает, но в кластерах почти в системы входит слишком много пабайди"""

df = pd.read_feather(os.path.join("data", "240312", "clusters.feather"))
print(df)
print(df.info())
sys_pub_df = df[["SysID", "ParentPubList"]]
sys_pub_df["ParentPubList"] = sys_pub_df["ParentPubList"].apply(lambda x: tuple(x))
sys_pub_df.drop_duplicates(inplace=True)

from itertools import chain

sys_pub = [y for y in chain(*[[(s, x) for x in p] for s, p in list(sys_pub_df.itertuples(index=False))])]

sys_pub_unique = list(set(sys_pub))
print(sys_pub_unique[:10])
print(len(sys_pub_unique))


sys_pub_unique_df = pd.DataFrame(sys_pub_unique, columns=["sys", "pub"])

print(sys_pub_unique_df)
# sys_pub_unique_df.to_csv(os.path.join("data", "240312", "sys_pub.csv"), index=False)

# df.to_csv(os.path.join("clusters", "sys_1_thinned_questions.csv"), sep="\t", index=False)'''