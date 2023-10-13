import mysql.connector
import pandas as pd
import csv
pd.set_option('display.max_columns', 100)

conx = mysql.connector.connect(
  host="localhost",
  user="xilena",
  password="password",
  database="workshop2"
)

mycursor = conx.cursor()

df= pd.read_csv("the_grammy_awards.csv")

print(df)
print(df.dtypes)

#Me estaba pniendo mucho problema por el varchar, así que decidí rectificar cual es el record de mayor cantidad de caracteres en la columna
# max_length = df['workers'].str.len().max()
# print(max_length) # = 701

# max_length_artist = df['artist'].str.len().max()
# print(max_length_artist)# = 227

# max_length_category = df['category'].str.len().max()
# print(max_length_category)#=104

# max_length_img = df['img'].str.len().max()
# print(max_length_img)#=263

# max_length_nominee = df['nominee'].str.len().max()
# print(max_length_nominee)#=136
#######################


mycursor.execute("""
CREATE TABLE IF NOT EXISTS grammys (
  year VARCHAR(10),
  title VARCHAR(50),
  published_at datetime,
  updated_at datetime,
  category VARCHAR(105),
  nominee VARCHAR(140),
  artist varchar(230),
  workers varchar(710),
  img varchar(270),
  winner boolean
)
""")

consulta = "INSERT INTO grammys (year,title,published_at,updated_at,category,nominee,artist,workers,img,winner) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

with open('the_grammy_awards.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    next(reader)
    for row in reader:
        winner_value = 1 if row[9] == 'True' else 0
        datos = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], winner_value)
        mycursor.execute(consulta, datos)

conx.commit()
mycursor.close()
conx.close()
