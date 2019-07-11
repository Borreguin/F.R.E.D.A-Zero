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

from numpy import nan
import pandas as pd
import matplotlib.pyplot as plt
import os
from my_lib.hmm import visualization_util as vi

""" script path"""
file_to_read = "household_power_consumption.pkl"
script_path = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_path, "data", file_to_read)

dataset = pd.read_pickle(file_path) # .iloc[:4000] # take only 4000 samples to test
# daterange = pd.date_range("2006-12-17", "2010-11-26", freq="15T")
dataset.drop_duplicates(inplace=True)
#df_sampled = pd.DataFrame(index=daterange)

#df_result = pd.concat([df_sampled, dataset], axis=1)

#for col in df_result:
    # df_result[col] = pd.to_numeric(df_result[col])
#    df_result[col] = df_result[col].interpolate(method="time",limit=4,limit_area="inside")

df_result = dataset.resample('15T')# .max()
df_result = df_result.apply(lambda x: x.quantile(0.90))

# elimina datos no existentes (NAN values)
df_result.dropna(inplace=True)
# df_result = df_result.loc[df_result.index.isin(daterange)]

n_col = 2
figsize = (24, 7)
fig, axes = None, None
figures = list()
df_final = pd.DataFrame(columns=df_result.columns)
for ix, col in enumerate(df_result.columns):
    idx = ix % n_col
    if idx == 0:
        fig, axes = plt.subplots(nrows=1, ncols=n_col, figsize=figsize)
    df_inter = pd.DataFrame(columns=["re_sampled"], index=df_result.index)
    df_inter["re_sampled"] = df_result[col]
    to_plot = pd.concat([dataset[col], df_inter], axis=1)
    to_plot["re_sampled"] = to_plot["re_sampled"].interpolate(method="time")
    df_final[col] = df_result[col]
    if not to_plot.empty:
        to_plot.iloc[:3000].plot(ax=axes[idx])
    if ((ix + 1) % n_col == 0) and fig is not None:
        figures.append(fig)
        plt.tight_layout()
        plt.show()
        plt.close()

vi.save_as_pdf(figures,  os.path.join(script_path, "output"), "_re_sampled_trends.pdf")

"""
for col in dataset:
    df_to_plot = dataset[[col]].copy()
    df_to_plot["interpolated"] = df_result[col]
    df_to_plot.plot()
    plt.show()
"""

figures = vi.superposed_profiles(df_result, 3, figsize=(24, 7))
vi.save_as_pdf(figures, os.path.join(script_path, "output"), file_to_read.replace("pkl", "pdf"))

df_final.to_pickle(os.path.join(script_path, "data", file_to_read.replace(".pkl", "_re_sampled.pkl")))
