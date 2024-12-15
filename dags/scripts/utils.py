import sqlalchemy
from sqlalchemy import text
import pandas as pd

CONN_DICT = {
    'dbname': 'movies', 
    'user': 'airflow', 
    'password': 'airflow', 
    'port': 5432, 
    'host': 'postgres',
    'options': "-c client_encoding=UTF8"
}


def upload_table(table_name, dfr, conn_dict=CONN_DICT, batch_size=1000000):
    table_name = table_name.replace("-", "_")
    df = dfr.rename(columns={k: k.lower() for k in dfr.columns})
    conn_str = f"postgresql+psycopg2://{conn_dict['user']}:{conn_dict['password']}@{conn_dict['host']}:{conn_dict['port']}/{conn_dict['dbname']}"
    con = sqlalchemy.create_engine(conn_str)
    try:
        if len(df) > batch_size:
            df.iloc[:batch_size].to_sql(table_name, con, if_exists='fail')
            for i in range(batch_size, len(df), batch_size):
                df.iloc[i:i + batch_size].to_sql(table_name, con, if_exists='append')
                print(f"Table batch {i}:{i + batch_size} uploaded")
        else:
            df.to_sql(table_name, con, if_exists='fail')

        print(f"Table {table_name} uploaded")
    except ValueError as e:
        print(f'Table {table_name} already exists')

def get_table_names(conn_dict=CONN_DICT):
    conn_str = f"postgresql+psycopg2://{conn_dict['user']}:{conn_dict['password']}@{conn_dict['host']}:{conn_dict['port']}/{conn_dict['dbname']}"
    con = sqlalchemy.create_engine(conn_str)
    res = []
    with con.connect() as connection:
        result = connection.execute(text("select tablename from pg_catalog.pg_tables where schemaname='public';"))
        for row in result:
            res.append(row.tablename)
    return res

def get_table(table_name, fields=None, conn_dict=CONN_DICT):
    conn_str = f"postgresql+psycopg2://{conn_dict['user']}:{conn_dict['password']}@{conn_dict['host']}:{conn_dict['port']}/{conn_dict['dbname']}"
    con = sqlalchemy.create_engine(conn_str)
    if fields is None:
        fields_txt = "*"
    else:
        fields_txt = ", ".join(fields)
    print(f"Start getting table {table_name}")
    my_df = pd.read_sql_query(sql=f"select {fields_txt} from {table_name}", con=con)
    return my_df