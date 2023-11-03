import os
import asyncio
import pandas as pd
from src.storage import ElasticClient

async def es_update(index: str, data_dicts: list[dict]):
    """
    Main function: pipline for update data in es
    """
    
    es = ElasticClient()
    
    try:
        es.delete_index(index)
    except:
        print("There no index {}".format(index))
    
    await es.create_index(index)
    await es.add_docs(index, data_dicts)


if __name__ == "__main__":
    queries_df = pd.read_csv(os.path.join(os.getcwd(), "data", "queries_for_train.csv"), sep="\t")
    queries_dicts = queries_df.to_dict(orient="records")
    
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(es_update("dataset_queries", queries_dicts))
    except:
        loop.close()