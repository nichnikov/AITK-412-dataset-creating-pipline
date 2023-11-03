import os
import pandas as pd
from itertools import chain
from src.texts_processing import TextsTokenizer
from src.config import logger
from sentence_transformers import SentenceTransformer, util

tokenizer = TextsTokenizer()
model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')

qrs_df = pd.read_csv(os.path.join("data", "queries.tsv"), sep="\t")
fa_ids = list(set(qrs_df["id"]))

max_examples = 10 # столько примеров будет отбираться из быстрого ответа
threshold = 0.9 # векторы примеров должны быть не ближе друг ко другу, чем это косинусное расстояние

class QueriesAnalysis:
    def __init__(self, model: SentenceTransformer, tokenizer: TextsTokenizer) -> None:
        self.model = model
        self.tokenizer = tokenizer

    def dissimilar_queries(self, max_examples: int, fa_ids: int, quries: [str]) -> []:
        """
        Функция, отбирающая для каждого быстрого ответа только непохожие вопросы (с точки зрения Сберт-трансформера)
        max_examples - столько примеров будет отбираться из быстрого ответа
        threshold - векторы примеров должны быть не ближе друг ко другу, чем это косинусное расстояние
        fa_ids - уникальные айди быстрых ответов
        max_group_len - максимальная длина вопросов в группе (у быстрого ответа)
        """
        lm_queries = [" ".join(lq) for lq in self.tokenizer(quries)]
        paphrs = util.paraphrase_mining(self.model, lm_queries)
        dissim_queries = []
        if paphrs:
            if paphrs[-1][0] >= 0.5: 
                # в группе вопросы должны быть относительно похожими (больше, чем на 0.5 для Сберта), 
                # иначе группа совсем разнородная и лучше ее не использовать для обучения нейронной сети
                not_sims_paphrs = [(p[1], p[2]) for p in paphrs[-max_examples:] if p[0] <= threshold]
                if not_sims_paphrs:
                    paphrs_not_sim_indx_1, paphrs_not_sim_indx_2 = zip(*not_sims_paphrs)
                    lm_queries_temp_1 = [lm_queries[i] for i in paphrs_not_sim_indx_1]
                    lm_queries_temp_2 = [lm_queries[i] for i in paphrs_not_sim_indx_2]
                    temp_paphrs_1 = util.paraphrase_mining(self.model, lm_queries_temp_1)
                    temp_paphrs_2 = util.paraphrase_mining(self.model, lm_queries_temp_2)                                 
                    temp_paphrs_1_indx = [p for p in temp_paphrs_1 if p[0] <= threshold]                 
                    temp_paphrs_2_indx = [p for p in temp_paphrs_2 if p[0] <= threshold]
                    qrs_indx1 = [(paphrs_not_sim_indx_1[p[1]], paphrs_not_sim_indx_1[p[2]]) for p in temp_paphrs_1_indx[-max_examples:]]
                    qrs_indx2 = [(paphrs_not_sim_indx_2[p[1]], paphrs_not_sim_indx_2[p[2]]) for p in temp_paphrs_2_indx[-max_examples:]]
                    unique_indexes = list(set([x for x in chain(*(qrs_indx1 + qrs_indx2))]))
                    if unique_indexes:
                        finished_lm_queries = [lm_queries[i] for i in unique_indexes]
                        finished_paphrs = util.paraphrase_mining(self.model, finished_lm_queries)
                        sifted_indexes = [unique_indexes[p[1]] for p in finished_paphrs if p[0] > threshold]
                        for idx in [i for i in unique_indexes if i not in sifted_indexes]:
                            dissim_queries.append(quries[idx])
                    else:
                        dissim_queries.append(quries[0])
                else:
                    dissim_queries.append(quries[0])
        return dissim_queries
    
    




"""    
queries_for_train = []
for num, fa_id in enumerate(fa_ids):
    print(num, "/", len(fa_ids))
    quries = list(qrs_df["query"][qrs_df["id"] == fa_id])
    if len(quries) <= 300:
        lm_queries = [" ".join(lq) for lq in tokenizer(quries)]
        paphrs = util.paraphrase_mining(model, lm_queries)
        temp_qrs_indx = []
        if paphrs:
            if paphrs[-1][0] >= 0.5:
                if paphrs:
                    not_sims_paphrs = [(p[1], p[2]) for p in paphrs[-max_examples:] if p[0] <= threshold]
                    if not_sims_paphrs:
                            paphrs_not_sim_indx_1, paphrs_not_sim_indx_2 = zip(*not_sims_paphrs)
                            lm_queries_temp_1 = [lm_queries[i] for i in paphrs_not_sim_indx_1]
                            lm_queries_temp_2 = [lm_queries[i] for i in paphrs_not_sim_indx_2]
                            temp_paphrs_1 = util.paraphrase_mining(model, lm_queries_temp_1)
                            temp_paphrs_2 = util.paraphrase_mining(model, lm_queries_temp_2)                                 
                            temp_paphrs_1_indx = [p for p in temp_paphrs_1 if p[0] <= threshold]                 
                            temp_paphrs_2_indx = [p for p in temp_paphrs_2 if p[0] <= threshold]
                            qrs_indx1 = [(paphrs_not_sim_indx_1[p[1]], paphrs_not_sim_indx_1[p[2]]) for p in temp_paphrs_1_indx[-max_examples:]]
                            qrs_indx2 = [(paphrs_not_sim_indx_2[p[1]], paphrs_not_sim_indx_2[p[2]]) for p in temp_paphrs_2_indx[-max_examples:]]
                            unique_indexes = list(set([x for x in chain(*(qrs_indx1 + qrs_indx2))]))
                            if unique_indexes:
                                finished_lm_queries = [lm_queries[i] for i in unique_indexes]
                                finished_paphrs = util.paraphrase_mining(model, finished_lm_queries)
                                sifted_indexes = [unique_indexes[p[1]] for p in finished_paphrs if p[0] > threshold]
                                for idx in [i for i in unique_indexes if i not in sifted_indexes]:
                                    queries_for_train.append({"id": fa_id, "query": quries[idx]})
                            else:
                                    queries_for_train.append({"id": fa_id, "query": quries[0]})
                    else:
                                queries_for_train.append({"id": fa_id, "query": quries[0]})

queries_for_train_df = pd.DataFrame(queries_for_train)
print(queries_for_train_df)
queries_for_train_df.to_csv(os.path.join("data", "queries_for_train.tsv"), sep="\t", index=False)"""