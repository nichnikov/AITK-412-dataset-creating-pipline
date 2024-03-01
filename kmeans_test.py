"""
from sklearn.cluster import KMeans
import numpy as np

X = np.array([[1, 2], [1, 4], [1, 0], [10, 3], [10, 4], [10, 0]])
kmeans = KMeans(n_clusters=2, random_state=0, n_init="auto").fit(X)
print(kmeans.labels_)
pr = kmeans.predict([[0, 0], [12, 3]])

print(pr)
print(kmeans.cluster_centers_)

from sklearn.metrics.pairwise import cosine_similarity
X = [[0, 0, 0], [1, 1, 1]]
Y = [[1, 0, 0], [1, 1, 1]]
print(cosine_similarity(X, Y))
"""

d = {"d": "D"}
print(list(d.keys())[0])