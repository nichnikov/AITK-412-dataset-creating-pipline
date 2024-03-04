import os
import asyncio
import pandas as pd
from src.config import logger
from src.storage import ElasticClient

df = pd.read_feather(os.path.join(os.getcwd(), "datasets", "dissimilar_queries_sys1.feather"))
print(df)

es = ElasticClient()

thinned_queries = "thinned_fa_questions"
indexes_files = [(thinned_queries, "sys_1_thinned_questions.csv")]

for index, file in indexes_files:
    df = pd.read_csv(os.path.join(os.getcwd(), "datasets", file), sep="\t")
    print("before print(df.shape)", df.shape)
    df.dropna(inplace=True)
    print("after print(df.shape)", df.shape)      

    data_dicts = df.to_dict(orient="records")
    
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(es.delete_index(index))
    except:
        logger.exception("There no index {}".format(index))
        # print("There no index {}".format(index))
    
    loop.run_until_complete(es.create_index(index))
    try:
        loop.run_until_complete(es.add_docs(index, data_dicts))
    except:
        loop.close()
    
loop.close()