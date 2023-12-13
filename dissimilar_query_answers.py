import os
import pandas as pd
from random import choice

dissim_queries_df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_dissimilar_queries.csv"), sep="\t")
sys_ids = set(list(dissim_queries_df["sys"]))

queries_df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_queries.csv"), sep="\t")
answers_df = pd.read_csv(os.path.join(os.getcwd(), "data", "231207", "dataset_answers.csv"), sep="\t")

queries_df["id"] = queries_df["id"].astype(int)
queries_df["sys"] = queries_df["sys"].astype(int)
print(queries_df.info())

"""вопросы не соответствующие ответам:"""
dissim_queries_answers = []
problems = []
for sys_id in sys_ids:
    id1_id2 = [tuple(sorted(tpl)) for tpl in dissim_queries_df[["id1", "id2"]][dissim_queries_df["sys"] == sys_id].itertuples(index=False)]
    id1_id2_uniq = set(id1_id2)
    for id1, id2 in id1_id2_uniq:
        qrs_df = queries_df[["query", "lem_query"]][(queries_df["sys"] == sys_id) & (queries_df["id"] == id1)]
        qrs_dcts = qrs_df.to_dict(orient="records")
        asw_df = answers_df[(answers_df["sys_id"] == sys_id) & (answers_df["ID"] == id2)]
        asw_dcts = asw_df.to_dict(orient="records")
        if qrs_dcts and asw_dcts:
            dissim_queries_answers.append({**choice(qrs_dcts), **choice(asw_dcts)})
        else:
            if not asw_dcts:
                problems.append({"problem": "empty answer", "sys": sys_id, "id": id2})
                print("empty answer with sys {} and id2 {}".format(str(sys_id), str(id2)))
            elif not qrs_dcts:
                print("empty queries with sys {} and id1 {}".format(str(sys_id), str(id1)))
                problems.append({"problem": "empty queries", "sys": sys_id, "id": id1})
            else:
                print("empty queries and answers with sys {}, id1 {}, id2 {}".format(str(sys_id), str(id1), str(id2)))
                problems.append({"problem": "empty queries and answers", "sys": sys_id, "id": [id1, id2]})
            
    

dissim_queries_answers_df = pd.DataFrame(dissim_queries_answers)
print(dissim_queries_answers_df)
dissim_queries_answers_df.to_csv(os.path.join(os.getcwd(), "data", "231207", "dissim_queries_answers.tsv"), sep="\t", index=False)
problems_df = pd.DataFrame(problems)
problems_df.to_csv(os.path.join(os.getcwd(), "data", "231207", "dissim_queries_answers_problems.tsv"), sep="\t", index=False)