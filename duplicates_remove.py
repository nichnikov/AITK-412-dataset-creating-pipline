"""
в рамках системы надо удалить близкие до сличения предложения с разными TemplateID
https://www.sbert.net/docs/package_reference/util.html 
"""

import os
import pandas as pd
from sentence_transformers import SentenceTransformer, util

model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')
df_sys1 = pd.read_feather(os.path.join("clusters", "sys_1_thinned_questions.feather"))
print(df_sys1)

sentences = df_sys1["LmQuery"].to_list()
template_ids = df_sys1["TemplateID"].to_list()

paraphrases = util.paraphrase_mining(model, sentences, show_progress_bar=True)

print(len(paraphrases))

paraphrases_score = []
for paraphrase in paraphrases:
    score, i, j = paraphrase
    if score > 0.95:
        paraphrases_score.append({
            "templateID1": template_ids[i],
            "templateID2": template_ids[j],
            "LmQuery1": sentences[i],
            "LmQuery2": sentences[j],
            "score": score
        })
    # print("{} \t\t {} \t\t {} \t\t {} \t\t Score: {:.4f}".format(template_ids[i], template_ids[j], sentences[i], sentences[j], score))

paraphrases_score_df = pd.DataFrame(paraphrases_score)
print(paraphrases_score_df)
paraphrases_score_df.to_csv(os.path.join("clusters", "sys_1_duplicates_questions.csv"), sep="\t", index=False)
