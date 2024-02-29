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
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from transformers import (AutoTokenizer, 
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

e5_tokenizer = AutoTokenizer.from_pretrained('intfloat/multilingual-e5-large')
e5_model = AutoModel.from_pretrained('intfloat/multilingual-e5-large').to('cuda')
kmeans_clusterer = KMeans(n_clusters=5, init='k-means++', n_init='auto', 
                   max_iter=300, tol=0.0001, verbose=0, random_state=None, copy_x=True, algorithm='lloyd')


sys1_df = pd.read_csv(os.path.join("data", "240228", "sys_1_clusters.tsv"), sep="\t")

for id in sys1_df["ID"].unique()[:50]:
    clusters = sys1_df[sys1_df["ID"] == id]["Cluster"].to_list()
    if len(clusters) > 10:
        vcs_tensors = e5_txs2enbs(e5_model, e5_tokenizer, clusters)
        vcs_numpy = np.array(vcs_tensors)
        cls = kmeans_clusterer.fit(vcs_numpy)
        
        for cls_num, center_arr in enumerate(cls.cluster_centers_):
            cls_queries = [q for q, lb in zip(clusters, cls.labels_.tolist()) if lb == cls_num]
            cs = cosine_similarity(center_arr.reshape(1, center_arr.shape[0]), vcs_numpy)
            print(cls_queries)
            print(sorted(list(zip(cs[0], cls_queries)), key=lambda x: x[0], reverse=True)[0])
            print("\n\n\n")
        
        
        
        