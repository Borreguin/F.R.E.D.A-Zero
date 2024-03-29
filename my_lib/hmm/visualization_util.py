""""
    Created by Roberto Sánchez A, based on the Master Thesis:
    "A proposed method for unsupervised anomaly detection for arg_from multivariate building dataset "
    University of Bern/Neutchatel/Fribourg - 2017
    Any copy of this code should be notified at rg.sanchez.arg_from@gmail.com
    to avoid intellectual property's problems.

    Not details about this code are included, if you need more information. Please contact the email above.
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""


import numpy as np
# import matplotlib as plt
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf
from cycler import cycler
import os
# from matplotlib.dates import YearLocator, MonthLocator
# import pylab as pl
# from IPython.display import display

# st.use('seaborn-colorblind')
# use of plotly:
# import plotly.offline as py
import pandas as pd
# import plotly.graph_objs as go  #important library for the plotly
# py.init_notebook_mode(connected=False) # run at the start of every ipython notebook to use plotly.offline
# from plotly import tools #to do subplots


# use of cufflinks:
# import cufflinks as cf
# from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
# init_notebook_mode(connected=False)
# cf.set_config_file(offline=True, world_readable=True, theme='ggplot')


def set_iCol_and_iRow(iCol, iRow, nrows, ncols):
    iCol += 1
    if iRow >= nrows:
        iRow = 0
    if iCol >= ncols:
        iCol = 0
        iRow += 1
    return iCol, iRow


def plot_profiles_24h(df_x, df_y, states_to_plot, yLim, units, n_col=4, figsize=(16, 24), nticks=10):
    nrows = int(np.ceil(len(states_to_plot) / n_col))
    n_samples = len(df_x.loc[df_x.index[0]])
    if n_samples == 24:
        labels = range(24)
    elif n_samples > 24:
        fc = n_samples / 24
        labels = [round(x / fc,1) for x in range(int(fc * n_samples))]
        # stp = int(n_samples / nticks)
        # labels = [x if ix %2 == 0 else "" for ix, x in zip(range(len(labels)), labels)]
    else:
        labels = range(n_samples)

    id_n = -1
    figures = list()
    for i in range(nrows):
        fig, axes = plt.subplots(nrows=1, ncols=n_col, figsize=figsize)
        medianprops = dict(linewidth=4, color='red')
        for j in range(n_col):
            id_n += 1

            if id_n < len(states_to_plot):
                n = states_to_plot[id_n]
                mask = df_y[df_y['hidden_states'] == n].index
                if (len(mask)) > 0:
                    df_to_plot = df_x.loc[mask]
                    df_to_plot.plot.box(ax=axes[j], notch=False, medianprops=medianprops, showfliers=True)
                    axes[j].set_ylim(yLim)
                    axes[j].set_xlabel('Hours')
                    # axes[-1][j].set_xlabel('Hours')
                    axes[j].set_ylabel('[ ' + units + ' ]')
                    axes[j].set_title('ID_= ' + str(n) + ' #Days=' + str(len(mask)))
                    axes[j].set_xticklabels(labels=labels, rotation=-90)

                for label in axes[j].get_xticklabels()[::2]:
                    label.set_visible(False)
        figures.append(fig)
        plt.tight_layout()
        plt.show()

    return figures


def superposed_profiles(df_x: pd.DataFrame, n_col=3, figsize=(16, 24)):
    figures = list()

    assert isinstance(df_x.index, pd.core.indexes.datetimes.DatetimeIndex)
    print(type(df_x.index))
    new_prop_cycle = cycler('color', [(0.1, 0.2, 0.5), (0.3, 0.3, 0.9), (0.1, 0.1, 0.1)]) \
                     * cycler('linewidth', [1., 1.5, 2.]) * cycler('alpha', [0.1, 0.2, 0.5])
    plt.rc('axes', prop_cycle=new_prop_cycle)
    font = {'family': 'times new roman',
            # 'weight' : 'bold',
            'size': 26}
    plt.rc('font', **font)

    columns = df_x.columns

    if len(columns) == 0:
        return list()
    elif len(columns) > 1 :
        # nrows = int(np.ceil(len(columns) / n_col))
        df_x["hour"] = [x.time() for x in df_x.index]
        df_x["date"] = [x.date() for x in df_x.index]
        fig, axes = None, None

        for ix, col in enumerate(columns):
            idx = ix % n_col
            if idx == 0:
                fig, axes = plt.subplots(nrows=1, ncols=n_col, figsize=figsize)
            df_to_plot = df_x.pivot(index="date", columns="hour", values=col)
            df_to_plot.dropna(inplace=True)
            if not df_to_plot.empty:
                df_to_plot.T.plot(legend=False, ax=axes[idx])
                axes[idx].set_title(col)

            if ((ix+1) % n_col == 0) and fig is not None:
                figures.append(fig)
                plt.tight_layout()
                plt.show()

        return figures


def save_as_pdf(figures, output_path, file_name):

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    path_to_save = os.path.join(output_path, file_name)

    pdf = matplotlib.backends.backend_pdf.PdfPages(path_to_save)
    for fig in figures:  # xrange(1, figure().number): ## will open an empty extra figure :(
        pdf.savefig(fig)
    pdf.close()
