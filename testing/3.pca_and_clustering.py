# Created by Roberto Sanchez at 6/29/2019
# -*- coding: utf-8 -*-
""""
    Created by Roberto Sánchez A, based on the Master Thesis:
    "A proposed method for unsupervised anomaly detection for a multivariate building dataset "
    University of Bern/Neutchatel/Fribourg - 2017
    Any copy of this code should be notified at rg.sanchez.a@gmail.com; you can redistribute it
    and/or modify it under the terms of the MIT License.

    The F.R.E.D.A project is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
    without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    MIT license terms for more details.

    If you need more information. Please contact the email above: rg.sanchez.a@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""


from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import DBSCAN
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


import pandas as pd
import matplotlib.pyplot as plt
import os
from my_lib.hmm import visualization_util as vi

""" script path"""
file_to_read = "household_power_consumption_re_sampled.pkl"
script_path = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_path, "data", file_to_read)

dataset = pd.read_pickle(file_path) #.iloc[:4000] # take only 4000 samples to test
print(dataset.info())
df_pivot = dataset.copy()
df_pivot["hour"] = [x.time() for x in df_pivot.index]
df_pivot["date"] = [x.date() for x in df_pivot.index]

n_col =2
figsize = (24,7)
for ix, col in enumerate(dataset.columns):
    fig, axes = plt.subplots(nrows=1, ncols=n_col, figsize=figsize)
    df = df_pivot.pivot(index="date", columns="hour", values=col)
    df.dropna(inplace=True)
    x = df.values
    # Standardizing the features
    x = StandardScaler().fit_transform(x)
    # PCA in 3 dimensions
    pca = PCA(n_components=3)
    principalComponents = pca.fit_transform(x)

    principalDf = pd.DataFrame(data=principalComponents,
                               columns=['principal component 1', 'principal component 2', 'principal component 3'])

    # cluster.estimate_bandwidth(x, quantile=0.3)

    """    
    for k in range(4, 20):
        # The silhouette_score gives the average value for all the samples.
        # This gives a perspective into the density and separation of the formed
        # clusters
        kmeans = KMeans(n_clusters=k, random_state=0).fit(x)
        silhouette_avg = silhouette_score(x, kmeans.labels_)
        print(k, silhouette_avg)
    """

    for k in range(1, 10):
        eps = k*10
        try:
            dbscan = DBSCAN(eps=eps, min_samples=2, leaf_size=2).fit(x)
            silhouette_avg = silhouette_score(x, dbscan.labels_)
            print(k, silhouette_avg)
        except Exception as e:
            print(e)