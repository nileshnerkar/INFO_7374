import pyodbc
import pandas as pd
import numpy as np
import warnings
import json
from sqlalchemy import create_engine
from fred import Fred

warnings.filterwarnings("ignore")

def create_Fred_Client(api='32b108ff32a3a99c173ef203cbc5cdaa'):
    try:
        fr = Fred(api_key=api, response_type='json')
        print('FRED client created')
    except Exception:
        print('Cannot create FRED Client')
        exit()
    return fr


def create_sql_engine(uid, password, server='pgdp-assignment-2-server.database.windows.net',
                      database='pgdp-assignment-2-db', driver='ODBC Driver 13 for SQL Server'):
    connectionstring = 'mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}'.format(
        uid=uid,
        password=password,
        server=server,
        database=database,
        driver=driver.replace(' ', '+'))

    try:
        engn = create_engine(connectionstring)
        print('SQL Alchemy Engine created')
    except Exception:
        print("Could not create SQL Alchemy Engine")
        exit()
    return engn


def get_series(fr, series_id):
    try:
        jsonstr = fr.series.observations(series_id=series_id)
        js = json.loads(jsonstr)
        df = pd.DataFrame(js['observations'])
    except Exception as e:
        print(e)

    df.index = [x for x in range(1, len(df) + 1)]
    return df


def fix_datatypes(df):
    for cols in list(df.columns):
        if cols == 'value':
            df[cols] = df[cols].apply(lambda x: x if x.replace('.', '', 1).isdigit() else -999.0).astype('float64')
        else:
            df[cols] = pd.to_datetime(df[cols])
    return df


def null_imputation(df):
    # Take mean of all non-null rows and impute in null rows
    mean_val = df[df['value'] != -999.0].mean()[0]
    df['value'] = df['value'].apply(lambda x: mean_val if x == -999.0 else x)

    # Impute null date with placeholder value
    date_val = pd.to_datetime('2099-01-01')
    df['date'] = df['date'].apply(lambda x: date_val if pd.isna(x) else x)
    df['realtime_start'] = df['realtime_start'].apply(lambda x: date_val if pd.isna(x) else x)
    df['realtime_end'] = df['realtime_end'].apply(lambda x: date_val if pd.isna(x) else x)

    return df


def get_max_value_from_db(engine, t):
    max_val = 0
    try:
        if engine.has_table(t):
            query = 'SELECT MAX([{table}].[index]) FROM [dbo].[{table}]'.format(table=t)
            dfsql = pd.read_sql(query, engine)

            if str(dfsql.values[0][0]) == 'None':
                print('No records present in table {}'.format(t))
            else:
                max_val = int(dfsql.values[0][0])

    except Exception as e:
        print(e)

    return max_val


def ingest():
    DATABASE_TABLES = ['RATES', 'INFLATION', 'ECONOMY', 'CREDIT', 'AGRICULTURE']
    SERIES = ['DGS10', 'CPIAUCSL', 'GDPC1', 'BAMLC0A4CBBBEY', 'DRFLACBN']

    engine = create_sql_engine(uid='database_login', password='password-123')
    fr = create_Fred_Client()

    for t, s in list(zip(DATABASE_TABLES, SERIES)):
        series_df = get_series(fr, series_id=s)
        m_val = get_max_value_from_db(engine, t)

        if len(series_df[series_df.index > m_val]) == 0:
            print('No new data in table {}'.format(t))
            continue

        data_to_be_pushed = series_df[series_df.index > m_val]

        try:
            data_to_be_pushed.to_sql(t, engine, if_exists='append')
            print('Pushed {a} rows into table {b}'.format(a=len(data_to_be_pushed), b=t))
        except Exception as e:
            print(e)

    clean(engine, DATABASE_TABLES)

def clean(engine, DATABASE_TABLES):
    for t in DATABASE_TABLES:
        if engine.has_table(t):
            cleaned_max_val = get_max_value_from_db(engine,t + '_Cleaned')
            query = 'SELECT * FROM [dbo].[{table}] WHERE [{table}].[index] > {cleaned_max_val}'.format(table=t,cleaned_max_val=cleaned_max_val)
            df = pd.read_sql(query, engine, index_col='index')
            df_1 = fix_datatypes(df)
            df_2 = null_imputation(df_1)
            df_2.index = [x for x in range(cleaned_max_val + 1, len(df_2) + 1)]

            try:
                df_2.to_sql(t + '_Cleaned', engine, if_exists='append')
                print('Cleaned {a} rows into table {b}'.format(a=len(df_2), b= t + '_Cleaned'))
            except Exception as e:
                print(e)

if __name__ == '__main__':
    ingest()