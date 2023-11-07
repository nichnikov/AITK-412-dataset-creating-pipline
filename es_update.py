import os
import asyncio
import pandas as pd
from src.storage import ElasticClient

es = ElasticClient()

queries_index = "dataset_queries"
answers_index = "dataset_answers"

indexes_files = [(answers_index, "dataset_answers.csv"), (queries_index, "dataset_queries.csv")]

for index, file in indexes_files:
    df = pd.read_csv(os.path.join(os.getcwd(), "data", file), sep="\t")
    print("before print(df.shape)", df.shape)
    df.dropna(inplace=True)
    print("after print(df.shape)", df.shape)      

    data_dicts = df.to_dict(orient="records")
    
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(es.delete_index(index))
    except:
        print("There no index {}".format(index))
    
    loop.run_until_complete(es.create_index(index))
    try:
        loop.run_until_complete(es.add_docs(index, data_dicts))
    except:
        loop.close()
    
loop.close()