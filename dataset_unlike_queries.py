"""
Создает датасет непохожих вопросов из вопросов с пересекающимися словами (токенами), но имеющие разные айди быстрых ответов.
    """

import os
import pandas as pd
from collections import namedtuple
from src.texts_processing import TextsTokenizer
from src.storage import ElasticClient

queries_df = pd.read_csv(os.path.join(os.getcwd(), "data", "queries_for_train.tsv"), sep="\t")
print(queries_df)

texts = list(queries_df["query"])

tknz = TextsTokenizer()
es = ElasticClient()

queries_dicts = queries_df.to_dict(orient="records")
print(queries_dicts[:5])


dataset = []
for num, d in enumerate(queries_dicts):
    lm_tx = " ".join(tknz([d["query"]])[0])
    search_result = es.texts_search("dataset_queries", "LemQueries", [lm_tx])
    temp_dataset = []
    for sd in search_result[0]["search_results"]:
        if len(temp_dataset) < 10: # не больше 10 непохожих пар для каждого входящего вопроса
            if sd["answer_id"] != d["answer_id"]:
                temp_dataset.append(sorted((str(d["answer_id"]), d["query"], str(sd["answer_id"]), sd["query"]))
                    )
    print(num, "/", len(queries_dicts), "quantity:", len(temp_dataset))
    dataset += temp_dataset

dataset_df = pd.DataFrame(dataset, columns=["answ_id1", "answ_id2", "query1", "query2"])
print(dataset_df.shape)
dataset_df.drop_duplicates(inplace=True)
print(dataset_df.shape)
dataset_df.to_csv(os.path.join(os.getcwd(), "data", "queries_dataset_0.tsv"), sep="\t", index=False)