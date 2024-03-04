import os
import re
import pandas as pd
from datetime import datetime
from src.storage import DataFromDB


db_credentials =  {
        "server_host": "statistics.sps.hq.amedia.tech",
        "user_name": "nichnikov_ro",
        "password": "220929SrGHJ#yu"}

db_con = DataFromDB(**db_credentials)

class DataUploading:
        def __init__(self, db_con) -> None:
                self.db_con = db_con
                self.rows = []

        def sys_data_upload(self, sys_id):
                today = datetime.today().strftime('%Y-%m-%d')
                rows = db_con.get_rows(int(sys_id), today)
                self.rows = [nt._asdict() for nt in rows]

        def get_clusters(self):
                # Извлечение вопросов (кластеров)
                clusters = [{"id": d["ID"], "query": d["Cluster"]} for d in self.data_dicts]
                return pd.DataFrame(clusters)
                # clusters_df.to_csv(os.path.join("data", "queries.tsv"), sep="\t", index=False)

        def get_answers(self):
                # Извлечение текстов ответов
                patterns = re.compile("&#|;|\xa0")
                answers = [{"ID": patterns.sub(" ", str(tpl[0])), "DocName": patterns.sub(" ", str(tpl[1])), "ShortAnswer": tpl[2]} 
                        for tpl in set([(d["ID"], d["DocName"], d["ShortAnswerText"]) 
                                        for d in self.data_dicts])]
                return pd.DataFrame(answers)
                # answers_df.to_csv(os.path.join("data", "answers.tsv"), sep="\t", index=False)


today = None
sys_ids = []
for  sys_id in sys_ids:
        rows = db_con.get_rows(int(sys_id), today)
        data_dicts = [nt._asdict() for nt in rows]

        # Извлечение вопросов (кластеров)
        clusters = [{"id": d["ID"], "query": d["Cluster"]} for d in data_dicts]
        print(clusters[:20])
        print(len(clusters))

        clusters_df = pd.DataFrame(clusters)
        print(clusters_df)
        clusters_df.to_csv(os.path.join("data", "231207", "queries.tsv"), sep="\t", index=False)

        # Извлечение текстов ответов
        patterns = re.compile("&#|;|\xa0")
        answers = [{"ID": patterns.sub(" ", str(tpl[0])), "DocName": patterns.sub(" ", str(tpl[1])), "ShortAnswer": tpl[2]} 
                for tpl in set([(d["ID"], d["DocName"], d["ShortAnswerText"]) 
                                for d in data_dicts])]

        answers_df = pd.DataFrame(answers)
        print(answers_df)
        answers_df.to_csv(os.path.join("data", "231207", "answers.tsv"), sep="\t", index=False)
        # &#