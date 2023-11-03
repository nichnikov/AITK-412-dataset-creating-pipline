from src.data_uploading import DataUploading
from src.storage import DataFromDB
from src.queries_analysis import QueriesAnalysis
from src.texts_processing import TextsTokenizer
from sentence_transformers import SentenceTransformer

db_credentials =  {
        "server_host": "statistics.sps.hq.amedia.tech",
        "user_name": "nichnikov_ro",
        "password": "220929SrGHJ#yu"}

db_con = DataFromDB(**db_credentials)
data_upload = DataUploading(db_con)

tokenizer = TextsTokenizer()
model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')

queries_analysis = QueriesAnalysis(tokenizer, model)