"""Module with util methods for notebooks. It is used only for
 analysis purpose in notebooks, nowhere else is project."""
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.preprocessing import MinMaxScaler


# 01_analysis_rym.ipynb
def plot_ascii_chart(df: pd.DataFrame):
    arist_is_ascii = df['artist'].apply(lambda x: str(x).isascii()).sum()
    album_is_ascii = df['album'].apply(lambda x: str(x).isascii()).sum()
    labels = ['True', 'False']

    fig, axs = plt.subplots(1, 2, constrained_layout=True, figsize=(8, 5))
    axs[0].pie([arist_is_ascii, len(df) - arist_is_ascii], labels=labels, autopct='%1.2f%%')
    axs[0].set_title(f'Is name of artist in ASCII')
    axs[0].set_title(f'Czy nazwa artysty jest zapisana w ASCII')
    axs[1].pie([album_is_ascii, len(df) - album_is_ascii], labels=labels, autopct='%1.2f%%')
    axs[1].set_title(f'Is name of album in ASCII')
    axs[1].set_title(f'Czy nazwa albumu jest zapisana w ASCII')

    plt.savefig(r"D:\repo\music-rating-ai\docs\05_rym_is_ascii.svg")
    plt.show()


def scale_ratings(df: pd.DataFrame):
    scaler = MinMaxScaler()
    df = df.copy().sort_values(by=['rating'], ignore_index=True)

    # Cut mins, as it has long tail.
    df = df[int(len(df) / 1000):]

    # Scale values
    df['scaled_rating'] = scaler.fit_transform(df[['rating']].values) * 5

    # Manual intervals from 1 to 10
    df['manual'] = df['scaled_rating']
    df.loc[df['scaled_rating'] < 1, 'manual'] = 1
    df.loc[df['scaled_rating'].between(1, 1.75), 'manual'] = 2
    df.loc[df['scaled_rating'].between(1.75, 2.25), 'manual'] = 3
    df.loc[df['scaled_rating'].between(2.25, 2.75), 'manual'] = 4
    df.loc[df['scaled_rating'].between(2.75, 3.15), 'manual'] = 5
    df.loc[df['scaled_rating'].between(3.15, 3.40), 'manual'] = 6
    df.loc[df['scaled_rating'].between(3.40, 3.75), 'manual'] = 7
    df.loc[df['scaled_rating'].between(3.75, 4), 'manual'] = 8
    df.loc[df['scaled_rating'].between(4, 4.3), 'manual'] = 9
    df.loc[df['scaled_rating'] > 4.3, 'manual'] = 10
    df['manual'] /= 2

    return df


def plot_box_plots(df: pd.DataFrame):
    _, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(9, 5))
    ax1.boxplot(df['rating'])
    ax1.set_title('Original rating')

    ax2.boxplot(df['scaled_rating'])
    ax2.set_title('Scaled rating')

    ax3.boxplot(df['manual'])
    ax3.set_title('Manual intervals rating')

    plt.savefig(r"D:\repo\music-rating-ai\docs\master_thesis\08_iqr_ocen.svg")
    plt.show()


# 02_analysis_spotify
def plot_album_stats(df: pd.DataFrame, title: str = ''):
    df = df.copy()
    fig, axs = plt.subplots(1, 3, figsize=(12, 4), constrained_layout=True)

    tmp = df['decade'].value_counts().sort_index()
    axs[0].pie(tmp, labels=tmp.index, autopct='%1.1f%%')
    axs[0].set_title('Albums by decade', fontsize=10)

    df['num of artists'] = df['artist'].str.count('/') + df['artist'].str.count(',') + 1
    df.loc[df['num of artists'] >= 2, 'num of artists'] = '2+'
    tmp = df['num of artists'].value_counts()
    axs[1].pie(tmp, labels=tmp.index, autopct='%1.3f%%', startangle=50)
    axs[1].set_title("Number of album's artists", fontsize=10)

    df.loc[df['number_of_new_tracks'].between(1,3), 'number_of_new_tracks'] = 3
    df.loc[df['number_of_new_tracks'] > 16, 'number_of_new_tracks'] = 17

    tmp = df['number_of_new_tracks'].value_counts().sort_index().reset_index()
    tmp['val'] = tmp['index'].astype(str)
    tmp.loc[tmp['index'] == 3, 'val'] = '<= 3'
    tmp.loc[tmp['index'] == 17, 'val'] = '> 16'
    axs[2].pie(tmp['number_of_new_tracks'], labels=tmp['val'], autopct='%1.1f%%')
    axs[2].set_title('Number of tracks', fontsize=10)

    fig.subtitle(title + f' ({df.shape[0]} records)', fontsize=13)
    plt.plot()