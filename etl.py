import pandas as pd
import json
import logging
import sys
sys.path.append('./transformations')
from transformations.transform_spotify import drop_unnamed, artists, bool_artists, count_second_artists, bye_duplicates, simplified_genre, drop_artists, drop_second_artists, drop_na, drop_unn_columns
from transformations.transform_grammys import drop_img, fill_nulls_parenthesis, fill_nulls_split, fill_nulls_worker, fill_nulls_category, cleaning_category, drop_na_rows, drop_unnecesary_columns, rename_column
from transformations.fixing_merge import filling_nominated, bool_nominated, fill_na, rename_columns_merge
import mysql.connector
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

def create_db_connection():
    with open('config_db.json') as config_json:
        config = json.load(config_json)

    conx = mysql.connector.connect(**config) 
    return conx


#################### SPOTIFY

def read_csv():
    df=pd.read_csv("./data/spotify_dataset.csv")
    logging.info("DF created: ", df)
    return df.to_json(orient='records')


def transform_csv(json_data):
    #Let´s take the data from the extrack function "read_csv()"
    logging.info("****MY JSON DATA IS: ", json_data)
    logging.info("****TYPE OF JSON_DATA: ", type(json_data))

    data = json_data 

    data = json.loads(data) #Convert the data to a python object (list of dictionaries) because it was a string
    df=pd.DataFrame(data)
    logging.info("****MY DATAFRAME", df)

    #Dropping Unnamed: 0 because is an unnecesary column
    df = drop_unnamed(df)

    #Separated artists to main_artist and secondary_artists
    df = artists(df)

    #Boolean that let us know if the track of that row has secondary artists or not
    df= bool_artists(df)

    #Column that tell us how many secondary artists have that row
    df= count_second_artists(df)

    #Dropping column artists because is not neccesary anymore
    df=drop_artists(df)

    #Dropping column second_artist
    df = drop_second_artists(df)

    #Bye duplicates according to the track_id, just one song by id
    df= bye_duplicates(df)

    #Simplified the music genres
    df= simplified_genre(df)

    #Deleting the row full of NaN values
    df=drop_na(df)

    df=drop_unn_columns(df)
    
    logging.info("****MY DATAFRAME SHAPE", df.shape)
    logging.info("****MY DATAFRAME", df)
    return df.to_json(orient='records')


################## GRAMMYS

def read_db():
    conx = create_db_connection()

    query = "SELECT * FROM grammys"
    df = pd.read_sql(query, con=conx)

    conx.close()

    logging.info("****MY DATAFRAME", df)

    return df.to_json(orient='records')

def transform_db(json_data):
    #Let´s take the data from the extrack function "read_db()"
    logging.info("****MY JSON DATA IS: ", json_data)
    logging.info("****TYPE OF JSON DATA: ", type(json_data))

    data = json_data
    data = json.loads(data)
    df=pd.DataFrame(data)
    logging.info("****MY DATAFRAME", df)

    #Dropping 'img' - unnecesary column
    df = drop_img(df)

    print("!!!!!!!!!!!!!!!!!!!!!" *10)
    print(df.isna().sum())

    #Filling nulls in 'artist' with the data in parenthesis in the column 'workers'
    df=fill_nulls_parenthesis(df)

    #Filling nulls in 'artist' with the first artist in 'workers' before a ; or a ,
    df=fill_nulls_split(df)

    #Filling nulls in 'artist' with the data in 'workers'
    df=fill_nulls_worker(df)

    #Cleaning data in 'category' column
    df=cleaning_category(df)

    #Filling nulls in 'artist' with the data in 'nominee' according to the categories that involve artists
    df=fill_nulls_category(df)

    #Dropping the NA remaining values 
    df=drop_na_rows(df)

    #Dropping unnecesary columns
    df=drop_unnecesary_columns(df)

    #Rename 'winner' to 'was_nominated'
    df=rename_column(df)

    logging.info("****MY DATAFRAME SHAPE", df.shape)
    logging.info("****MY DATAFRAME", df)
    return df.to_json(orient='records')



############# MERGE


def merge(data_csv, data_db):
    logging.info("****MY CSV DATA IS: ", data_csv)
    logging.info("****TYPE OF CSV DATA: ", type(data_csv))

    logging.info("****MY DB DATA IS: ", data_db)
    logging.info("****TYPE OF DB DATA: ", type(data_db))

    data1 = data_csv
    data1 = json.loads(data1)
    df_csv=pd.DataFrame(data1)
    logging.info("****MY DATAFRAME SHAPE", df_csv.shape)

    data2 = data_db
    data2 = json.loads(data2)
    df_db=pd.DataFrame(data2)
    logging.info("****MY DATAFRAME SHAPE", df_db.shape)

    #I focused on the songs, because I want an analysis of the parameters of the songs that are nominated in the grammys and those that are not.
    df_merge = df_csv.merge(df_db, how='left', left_on='track_name', right_on='nominee')


    #Filling the nulls of 'was_nominated' with 0 =False
    df_merge= filling_nominated(df_merge)

    #Changinf the data type of the column 'was_nominated' to get 0=False and 1=True
    df_merge=bool_nominated(df_merge)

    #Filling all the nulls with 'not applicable'
    df_merge=fill_na(df_merge)
    logging.info("****FILL NA REVISION:", df_merge['nominee'])
    logging.info("****FILL NA REVISION 2:", df_merge['year'])

    #Rename the columns to make them clearer 
    df_merge=rename_columns_merge(df_merge)
    logging.info("****REVISION DE COLUMNAS:", df_merge.columns)

    df_merge.to_csv("./data/merge.csv", index=False)

    return df_merge.to_json(orient='records')

############# LOAD:

def load(json_data):
    logging.info("****MY DATA IS: ", json_data)
    logging.info("****TYPE OF DATA: ", type(json_data))

    data = json.loads(json_data) 
    df = pd.DataFrame(data)
    logging.info("****MY DF IS: ", df)

    conx = create_db_connection()
    mycursor = conx.cursor()

    mycursor.execute("""
    CREATE TABLE IF NOT EXISTS music_nomination (
    id INT AUTO_INCREMENT PRIMARY KEY,
    track_id VARCHAR(800),
    album_name VARCHAR(800),
    track_name VARCHAR(800),
    popularity INT,
    duration_ms INT,
    explicit BOOLEAN,
    danceability FLOAT,
    energy FLOAT,
    loudness FLOAT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INT,
    track_genre VARCHAR(800),
    main_artist VARCHAR(800),
    has_secondary_artists BOOLEAN,
    count_second_artists INT,
    simplified_genre VARCHAR(800),
    year_of_nomination VARCHAR(800),
    title_of_nomination VARCHAR(800),
    category_of_nomination VARCHAR(800),
    nominee_to_grammys VARCHAR(800),
    artist_of_nomination VARCHAR(800),
    was_nominated BOOLEAN
);
""")

    for _, i in df.iterrows():
        consulta = """
            INSERT INTO music_nomination (
            track_id, 
            album_name, 
            track_name, 
            popularity, 
            duration_ms, 
            explicit, 
            danceability, 
            energy, 
            loudness, 
            speechiness, 
            acousticness, 
            instrumentalness, 
            liveness, 
            valence, 
            tempo, 
            time_signature, 
            track_genre, 
            main_artist, 
            has_secondary_artists, 
            count_second_artists, 
            simplified_genre, 
            year_of_nomination, 
            title_of_nomination, 
            category_of_nomination, 
            nominee_to_grammys, 
            artist_of_nomination, 
            was_nominated
        ) 
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """

        datos = (
            i['track_id'],
            i['album_name'],
            i['track_name'],
            i['popularity'],
            i['duration_ms'],
            i['explicit'],
            i['danceability'],
            i['energy'],
            i['loudness'],
            i['speechiness'],
            i['acousticness'],
            i['instrumentalness'],
            i['liveness'],
            i['valence'],
            i['tempo'],
            i['time_signature'],
            i['track_genre'],
            i['main_artist'],
            i['has_secondary_artists'],
            i['count_second_artists'],
            i['simplified_genre'],
            i['year_of_nomination'],
            i['title_of_nomination'],
            i['category_of_nomination'],
            i['nominee_to_grammys'],
            i['artist_of_nomination'],
            i['was_nominated']
        )

        mycursor.execute(consulta, datos)

    conx.commit()
    mycursor.close()
    conx.close()

    json_df = df.to_json(orient='records')

    return json_df



############## STORE:

credentials_file = './pydrive/credentials_module.json'

#Automatic authentication - would be necesary for our upload_file function
def login():
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_file #Configurar ubicación de las credenciales par autenticar la aplicación
    gauth = GoogleAuth() 
    gauth.LoadCredentialsFile(credentials_file) #Cargar credenciales almacenadas
    
    if gauth.credentials is None:
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
        
    gauth.SaveCredentialsFile(credentials_file)
    credentials = GoogleDrive(gauth)
    return credentials

def upload_file(path_file,id_folder):
    credentials = login()
    file = credentials.CreateFile({'parents': [{"kind": "drive#fileLink",\
                                                    "id": id_folder}]})
    file['title'] = path_file.split("/")[-1]
    file.SetContentFile(path_file)
    file.Upload()

def store(json_data):
    logging.info("****MY JSON DATA IS: ", json_data)
    logging.info("****TYPE OF JSON DATA: ", type(json_data))

    id_folder="1Y-8YJ6FSGWCsVPdzQ3k18H0v2eF_MWeq"
    path_file="./data/merge.csv"
    upload_file(path_file,id_folder)




if __name__ == '__main__':
    read_csv()
    transform_csv()
    read_db()
    merge()
    load()
    login()
    upload_file()
    store()


#REFERENCE for the STORE part: 
# MoonCode. (2021). Aprende a usar Google Drive con Python en 20 minutos 😃💻-Learn Python and Google Drive in 20 minutes [YouTube Video]. In YouTube. https://www.youtube.com/watch?v=ZI4XjwbpEwU&t=168s
