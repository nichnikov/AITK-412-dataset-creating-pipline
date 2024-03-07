"""
в рамках системы надо удалить близкие до сличения предложения с разными TemplateID
https://www.sbert.net/docs/package_reference/util.html 
"""

import os
import pandas as pd
from sentence_transformers import SentenceTransformer, util

model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')

for sys_id in [1, 2, 3, 4, 8, 10, 11, 13, 14, 15, 16, 21, 22, 27, 28, 34, 37, 45, 47, 50, 51, 54, 55]:
    in_fn = "_".join(["sys", str(sys_id), "thinned_questions.feather"])
    sys_queries_df = pd.read_feather(os.path.join("clusters", in_fn))
    print(sys_queries_df)

    sentences = sys_queries_df["LmQuery"].to_list()
    template_ids = sys_queries_df["TemplateID"].to_list()

    paraphrases = util.paraphrase_mining(model, sentences, show_progress_bar=True)

    print(len(paraphrases))

    paraphrases_score = []
    for paraphrase in paraphrases:
        score, i, j = paraphrase
        if score > 0.99:
            paraphrases_score.append({
                "templateID1": template_ids[i],
                "templateID2": template_ids[j],
                "LmQuery1": sentences[i],
                "LmQuery2": sentences[j],
                "score": score
            })
        # print("{} \t\t {} \t\t {} \t\t {} \t\t Score: {:.4f}".format(template_ids[i], template_ids[j], sentences[i], sentences[j], score))




    """
    Из входящих запросов (из thinned_questions) удалим те группы вопросов, которые имеют дубли в других группах
    Из двух групп, имеющих общие (пересекающиеся) вопросы, удаляем меньшую (в которой меньше вопросов)
    Большую оставляем
    """

    paraphrases_score_df = pd.DataFrame(paraphrases_score)
    tplids1_unique = paraphrases_score_df["templateID1"].unique()

    for num, tplid1 in enumerate(tplids1_unique):
        print(num, "/", len(tplids1_unique))
        tplids1_df = paraphrases_score_df[paraphrases_score_df["templateID1"] == tplid1]
        tplids2_unique = paraphrases_score_df["templateID2"].unique()
        tplid1_quntity = tplids1_df.shape[0]
        for tplid2 in tplids2_unique:
            tplids2_df = sys_queries_df[sys_queries_df["TemplateID"] == tplid2]
            tplid2_quntity = tplids2_df.shape[0]
            if tplid1_quntity >= tplid2_quntity:
                sys_queries_df = sys_queries_df[sys_queries_df["TemplateID"] != tplid2]
            else:
                sys_queries_df = sys_queries_df[sys_queries_df["TemplateID"] != tplid1]


    
    out_out = "_".join(["sys", str(sys_id), "thinned_questions_without_duplicates.feather"])
    sys_queries_df.to_feather(os.path.join("clusters", out_out))
    # sys_queries_df.to_csv(os.path.join("clusters", "sys_1_thinned_questions_without_duplicates.csv"), sep="\t", index=False)
    # paraphrases_score_df.to_csv(os.path.join("clusters", "sys_1_duplicates_questions.csv"), sep="\t", index=False)
