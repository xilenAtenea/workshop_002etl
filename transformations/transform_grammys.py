import pandas as pd
import re

def drop_img(df):
    df = df.drop(['img'], axis=1)
    return df

def fill_nulls_parenthesis(df):
    condition = df['artist'].isnull() & df['workers'].str.contains(r'\(.*\)')
    df.loc[condition, 'artist'] = df.loc[condition, 'workers'].apply(lambda x: re.search(r'\((.*?)\)', x).group(1) if isinstance(x, str) and re.search(r'\((.*?)\)', x) else None)
    return df

def fill_nulls_split(df):
    condition= df['workers'].str.contains('[;,]', na=False) & ~df['workers'].str.contains(r'\(.*\)', na=False) & df['artist'].isnull()
    df.loc[condition, 'artist'] = df.loc[condition, 'workers'].str.split('[;,]').str[0].str.strip()
    return df

def fill_nulls_worker(df):
    condition = df['artist'].isnull() & ~df['workers'].isnull()
    df.loc[condition, 'artist'] = df.loc[condition, 'workers']
    return df

def cleaning_category(df):
    df['category'] = [i.lower().replace('(', '').replace(')', '').replace('-', ' ').replace(',', '') for i in df['category']]
    return df

selected_categories = [
    'best new artist',
    'producer of the year non classical',
    'producer of the year classical',
    'remixer of the year non classical',
    'producer of the year',
    'classical producer of the year',
    'best classical vocal soloist',
    'best new classical artist',
    'best new artist of the year',
    'best producer of the year',
    'best new country & western artist',
    'most promising new classical recording artist',
    'best new artist of 1964',
    'best new country & western artist of 1964',
    'best new artist of 1963',
    'best new artist of 1962',
    'best new artist of 1961',
    'best new artist of 1960',
    'best new artist of 1959'
]

def fill_nulls_category(df):
    df.loc[(df['category'].isin(selected_categories)) & (df['artist'].isnull()), 'artist'] = df['nominee']
    return df

def drop_na_rows(df):
    df.dropna(subset=['artist', 'nominee'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def drop_unnecesary_columns(df):
    df.drop(['workers', 'updated_at', 'published_at'], axis=1, inplace=True)
    return df

def rename_column(df):
    df.rename(columns={'winner': 'was_nominated'}, inplace=True)
    return df




