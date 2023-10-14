import pandas as pd

def drop_unnamed(df):
    df.drop(['Unnamed: 0'], axis=1, inplace=True)
    return df

def artists(df):
    #Separated artists to main_artist and secondary_artists
    df[['main_artist', 'second_artists']] = df['artists'].str.split(';', n=1, expand=True)
    return df

def bool_artists(df):
    #Boolean that let us know if the track of that row has secondary artists or not
    df['has_secondary_artists'] = df['second_artists'].notna()
    return df

def count_second_artists(df):
    #Column that tell us how many secondary artists have that row
    df['count_second_artists'] = df['second_artists'].apply(lambda x: 0 if pd.isna(x) else x.count(';') + 1)
    return df

def drop_artists(df):
    #Dropping column artists because is not neccesary anymore - replaced by the column 'main_artist'
    df.drop(['artists'], axis=1, inplace=True)
    return df

def drop_second_artists(df):
    #Dropping column second_artists because is not neccesary anymore - replaced by the columns 'has_second_artists' and 'count_second_artists'
    df.drop(['second_artists'], axis=1, inplace=True)
    return df

def bye_duplicates(df):
    #Bye duplicates according to the track_id, just one song by id 
    df = df.loc[~df.duplicated(subset=['track_id'])].reset_index(drop=True)
    return df


genre_general = {
    'Rock': ['alt-rock', 'alternative', 'hard-rock', 'grunge', 'punk', 'rock', 'rock-n-roll', 'j-rock', 'psych-rock', 'punk-rock'],
    'Metal' : ['hardcore', 'heavy-metal', 'black-metal', 'metal', 'metalcore'],
    'Pop': ['pop', 'power-pop'],
    'Hip-Hop/R&B': ['hip-hop', 'r-n-b', 'trip-hop'],
    'Electronic': ['chicago-house', 'breakbeat', 'dance', 'dancehall', 'deep-house', 'detroit-techno', 'disco', 'edm', 'electro', 'electronic', 'techno', 'trance', 'dubstep', 'minimal-techno'],
    'Latin': ['latin', 'salsa', 'samba', 'reggaeton', 'tango'],
    'Other': [
        'acoustic', 'afrobeat', 'ambient', 'anime', 'bluegrass', 'blues', 'brazil', 'british',
        'cantopop', 'children', 'chill', 'classical', 'club', 'comedy', 'country', 'disney', 'drum-and-bass', 'dub', 'emo',
        'folk', 'forro', 'french', 'funk', 'garage', 'german', 'gospel', 'goth', 'grindcore', 'groove', 'guitar', 'happy',
         'hardstyle', 'honky-tonk', 'house', 'idm', 'indian', 'indie', 'indie-pop', 'industrial',
        'iranian', 'j-dance', 'j-idol', 'j-pop', 'jazz', 'k-pop', 'kids', 'malay', 'mandopop',
        'mpb', 'new-age', 'opera', 'pagode', 'party', 'piano', 'pop-film', 'progressive-house',
        'rockabilly', 'romance', 'sad', 'sertanejo', 'show-tunes', 'singer-songwriter', 'ska', 'sleep', 'soul', 'spanish', 'study',
        'swedish', 'synth-pop', 'techno', 'turkish', 'world-music'
    ]
}


def simplified_genre(df):
    def assign_simplified_genre(genre):
        for category, genre_list in genre_general.items():
            if genre in genre_list:
                return category
        return 'Other'

    df['simplified_genre'] = df['track_genre'].apply(assign_simplified_genre)

    return df


def drop_na(df):
    #Deleting the row full of NaN values:
    df.drop(df[df['track_id'] == '1kR4gIb7nGxHPI3D2ifs59'].index, axis=0, inplace=True)
    return df

def drop_unn_columns(df):
    df.drop(['mode', 'key'], axis=1, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


