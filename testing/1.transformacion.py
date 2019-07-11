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
import os

""" script path"""
script_path = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_path, "data", "household_power_consumption.zip")

dataset = pd.read_csv(file_path, sep=';', header=0, low_memory=False,
                   infer_datetime_format=True, parse_dates={'datetime':[0,1]},
                      index_col=['datetime'], compression= "zip")

# mark all missing values
dataset.replace('?', nan, inplace=True)
# add a column for for the remainder of sub metering
values = dataset.values.astype('float32')
# interpolv e 1 value
for col in dataset:
    dataset[col] = pd.to_numeric(dataset[col])
    dataset[col] = dataset[col].interpolate(method="time", limit=1)

dataset['sub_metering_4'] = (values[:,0] * 1000 / 60) - (values[:,4] + values[:,5] + values[:,6])
# save updated dataset
to_save_path = os.path.join(script_path, "data", "household_power_consumption.pkl")
dataset.to_pickle(to_save_path)


