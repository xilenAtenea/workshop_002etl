import pandas as pd
"""Note: 
Rows that are filled with 'N/A' (Not Applicable) in the string data 
type columns, after merging with the DataFrame 'df_db', indicate that 
there is no information available for those songs regarding Grammy 
nominations between the years 1958 and 2019. This is explicitly reflected 
in the 'was_nominated' column, where the corresponding value for those 
rows is 'False'. Similarly in the 'year' column that is replaced by '-1'. This 
helps to represent that there were no Grammy nominations for songs in those 
years and that specific information is no available on those songs.
 """

def filling_nominated(df):
    df['was_nominated'].fillna(0, inplace=True)
    return df

#Transformed 0=False and 1=True
def bool_nominated(df):
    df['was_nominated'] = df['was_nominated'].astype(bool)
    return df

#Handle nulls after the merge
def fill_na(df):
    def fill_logic(col):
        if col.name == "year":
            return col.fillna(-1)
        else:
            return col.fillna("Not Applicable")

    df = df.apply(fill_logic)
    return df

#Rename columns
def rename_columns_merge(df):
    df = df.rename(columns={'year':'year_of_nomination', 'title':'title_of_nomination', 'category':'category_of_nomination', 'nominee':'nominee_to_grammys','artist':'artist_of_nomination'})
    return df

