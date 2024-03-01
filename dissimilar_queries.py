"""

Избавление от похожих вопросов (кластеров) у каждого "Быстрого ответа" с помощью кластеризации: 
вопросы каждого быстрого ответа:
- векторизуются, 
- кластеризуются, 
- находится центр каждого кластера
- ближайший к центру кластера вопрос остается в выборке (остальные отбрасываются)

"""

import os
import pandas as pd
import numpy as np
from torch import Tensor
from src.start import tokenizer
from src.config import logger
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from transformers import (
    AutoTokenizer, 
    AutoModel, 
    XLMRobertaModel, 
    XLMRobertaTokenizerFast
                          )
import torch.nn.functional as F
import torch


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


def average_pool(last_hidden_states: Tensor, attention_mask: Tensor) -> Tensor:
    last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
    return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]


def e5_txs2enbs(model: XLMRobertaModel, e5_tokenizer: XLMRobertaTokenizerFast, texts: list[str]):
        """переводит тексты в векторы для модели е5"""
        txts_chunks = chunks(texts, 5)
        vectors = []
        for txs in  txts_chunks:
            batch_dict = e5_tokenizer(txs, max_length=512, padding=True, 
                                            truncation=True, return_tensors='pt').to('cuda')
            outputs = model(**batch_dict)
            embeddings = average_pool(outputs.last_hidden_state, batch_dict['attention_mask'])
            embeddings = F.normalize(embeddings, p=2, dim=1)
            vectors += [torch.tensor(emb, device='cpu') for emb in  embeddings]
        return vectors


def centers_quantity(cls_len):
    """Определение количества оставляемых от каждого БО вопросов (количество кластеров)"""
    
    if cls_len <= 3:
        return False
    
    elif cls_len >= 50:
        return cls_len // 10
    
    elif cls_len >= 10:
        return cls_len // 5
    
    elif cls_len >= 3:
        return cls_len // 2
    

e5_tokenizer = AutoTokenizer.from_pretrained('intfloat/multilingual-e5-large')
e5_model = AutoModel.from_pretrained('intfloat/multilingual-e5-large').to('cuda')


for sys_id in [2, 3, 4, 8, 10, 11, 13, 14, 15, 16, 21, 22, 27, 28, 34, 37, 45, 47, 50, 51, 54, 55]:
    fn ="_".join(["sys", str(sys_id), "clusters.tsv"])
    sys_df = pd.read_csv(os.path.join("data", "240228", fn), sep="\t")
    sys_df = sys_df[["ID", "Cluster", "LemCluster"]]
    sys_df.dropna(inplace=True)
            
    qrs_q = 0
    thinned_questions = []
    rest_questions = []

    for num, id in enumerate(sys_df["ID"].unique()):

        logger.info(" ".join(["Sys:", str(sys_id), "Status:", str(num), "from", str(len(sys_df["ID"].unique()))]))

        """Нужно сначала удалить все лемматизированные дубли:"""
        clusters_ = list(sys_df[sys_df["ID"] == id][["Cluster", "LemCluster"]].itertuples(index=False, name=None))
        clusters_dct = {" ".join(sorted(l_cl.split())): cl for cl, l_cl in clusters_}
        
        clusters = list(clusters_dct.keys())
        qrs_q += len(clusters)
        n_cls = centers_quantity(len(clusters))
        
        if n_cls:
            kmeans_clusterer = KMeans(n_clusters=n_cls, init='k-means++', n_init='auto', 
                    max_iter=300, tol=0.0001, verbose=0, random_state=None, copy_x=True, algorithm='lloyd')
            
            vcs_tensors = e5_txs2enbs(e5_model, e5_tokenizer, clusters)
            vcs_numpy = np.array(vcs_tensors)
            cls = kmeans_clusterer.fit(vcs_numpy)
            
            for cls_num, center_arr in enumerate(cls.cluster_centers_):
                cls_queries = [q for q, lb in zip(clusters, cls.labels_.tolist()) if lb == cls_num]
                cs = cosine_similarity(center_arr.reshape(1, center_arr.shape[0]), vcs_numpy)
                cls_center = sorted(list(zip(cs[0], cls_queries)), key=lambda x: x[0], reverse=True)[0]
                query_for_dataset = cls_center[1]
                
                thinned_questions.append((id, query_for_dataset, clusters_dct[query_for_dataset]))
                rest_questions += [(id, q, clusters_dct[q]) for q in cls_queries if q != query_for_dataset]

        else:
            for cl in clusters:
                thinned_questions.append((id, cl, clusters_dct[cl]))        
            

    logger.info(" ".join(["Quantity:", str(qrs_q), str(len(thinned_questions))]))

    thinned_questions_df = pd.DataFrame(thinned_questions, columns=["TemplateID", "LmQuery", "Query"])
    thinned_questions_df.to_csv(os.path.join("datasets", "sys_" + str(sys_id) + "_thinned_questions.csv"), 
                                sep="\t", index=False)

    rest_questions_df = pd.DataFrame(rest_questions, columns=["TemplateID", "LmQuery", "Query"])
    rest_questions_df.to_csv(os.path.join("datasets", "sys_" + str(sys_id) + "_rest_questions.csv"), 
                             sep="\t", index=False)
