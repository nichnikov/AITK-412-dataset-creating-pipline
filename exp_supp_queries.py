"""Подготовка тестовых выборок: Вопросы ЭП - эталоны Быстрых ответов для валидации"""
import os
import pandas as pd

df = pd.read_feather(os.path.join("data", "240311", "es_queries_answers.feather"))
print(df)