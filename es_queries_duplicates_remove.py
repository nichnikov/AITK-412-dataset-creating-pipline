"""
в рамках системы надо удалить близкие до сличения предложения из вопросов в экспертную поддержку (длинных вопросов)
https://www.sbert.net/docs/package_reference/util.html 
"""

"""

Избавление от похожих вопросов (кластеров) у каждого "Быстрого ответа" с помощью кластеризации: 
вопросы каждого быстрого ответа:
- векторизуются, 
- кластеризуются, 
- находится центр каждого кластера
- ближайший к центру кластера вопрос остается в выборке (остальные отбрасываются)

"""

import os
import numpy as np
import pandas as pd
from itertools import groupby
from operator import itemgetter
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer


def cluster_name_number(vectors: np.array) -> np.array:
    """Function get vectors, finds vector most close to average of vectors and returns it's number."""
    # weight_average_vector = np.average(vectors, axis=0, weights=vectors)
    weight_average_vector = np.average(vectors, axis=0)
    weight_average_vector_ = weight_average_vector.reshape(1, weight_average_vector.shape[0])
    distances_from_average = cosine_similarity(vectors, weight_average_vector_)
    return np.argmax(distances_from_average)


def grouped_func(data: list) -> [{}]:
    """Function groups input list of data with format: [(label, vector, text)]
    into list of dictionaries, each dictionary of type:
    {
    label: label,
    texts: list of texts correspond to label
    vectors_matrix: numpy matrix of vectors correspond to label
    }
    """
    data = sorted(data, key=lambda x: x[0])
    grouped_data = []
    for key, group_items in groupby(data, key=itemgetter(0)):
        d = {"label": key, "texts": []}
        temp_vectors = []
        for item in group_items:
            temp_vectors.append(item[1])
            d["texts"].append(item[2])
        d["vectors_matrix"] = np.vstack(temp_vectors)
        grouped_data.append(d)
    return grouped_data


def clustering_func(vectorizer: SentenceTransformer, clusterer: AgglomerativeClustering, texts: []) -> {}:
    """Function for text collection clustering"""
    vectors = vectorizer.encode([x.lower() for x in texts])
    clusters = clusterer.fit(vectors)
    data = [(lb, v, tx) for lb, v, tx in zip(clusters.labels_, vectors, texts)]
    grouped_data = grouped_func(data)
    result_list = []
    for d in grouped_data:
        label = str(d["label"])
        title_number = cluster_name_number(d["vectors_matrix"])
        title = d["texts"][title_number]
        cluster_size = len(d["texts"])
        result_list += [{"label": label, "title": title, "text": tx, "size": cluster_size} for tx in d["texts"]]
    return result_list


clusterer = AgglomerativeClustering(n_clusters=None, distance_threshold=0.8,
                                            memory=os.path.join("cache"))

vectorizer = SentenceTransformer('distiluse-base-multilingual-cased-v1')


        

queries_df = pd.read_csv(os.path.join("data", "bss_queries_answers.csv"), sep="\t")
sentences = queries_df["text"][queries_df["text_len"] <= 5].to_list()

queries_by_group = clustering_func(vectorizer, clusterer, [str(tx) for tx in sentences])
print(queries_by_group)

queries_by_group_df = pd.DataFrame(queries_by_group)
print(queries_by_group_df)
queries_by_group_df.to_csv(os.path.join("data", "short_queries_groups.csv"), sep="\t")