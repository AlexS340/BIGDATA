from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.decorators import task

import pendulum
import pandas as pd
import os
import matplotlib.pyplot as plt
from ast import literal_eval
import numpy as np

from scripts.utils import upload_table, get_table_names, get_table

def_path = "/opt/airflow"

with DAG(
    "ETL_process",
    default_args={"retries": 2},
    params = {
        "folder_path": Param(title="Path to table", description="Path after path to airflow project (e.g. data/)", type="string")
    },
    description="DAG for automatic extract, transform and load movies data",
    schedule=None,#"@daily",
    start_date=pendulum.datetime(2024, 12, 12, tz="UTC"),
    catchup=False,
    tags=["ETL"],
) as dag:

    @task
    def insert_tables_raw_to_dds(**kwargs):
        params = kwargs['params']
        print(f"ALL PARAMS: {params}")
        
        path_to_folder = os.path.join(def_path, params['folder_path'])
        for file in os.listdir(path_to_folder):
            if file.endswith('.csv'):
                path_to_table = os.path.join(path_to_folder, file)
                table_name = os.path.splitext(os.path.basename(path_to_table))[0]
                print(f"path to table is {path_to_table}, table name is {table_name}")
                df = pd.read_csv(path_to_table)
                upload_table(table_name=table_name, dfr=df)

    @task
    def insert_tables_dds_to_ods(**kwargs):
        params = kwargs['params']
        table_names = get_table_names()
        if "ods_movies_meta" not in table_names:

            if "ratings" in table_names:
                r = get_table("ratings", fields=['movieid', 'rating'])
            else:
                raise KeyError("No table named raitings")
            if "movies_metadata" in table_names:
                m = get_table("movies_metadata", fields=['id', 'budget', 'genres', 'runtime'])
                m = m.rename(columns={"id": "movieid"})
            else:
                raise KeyError("No table named movies_metadata")
            
            m['movieid'] = m['movieid'].astype("str", errors='ignore')
            r['movieid'] = r['movieid'].astype("str", errors='ignore')
            
            # m['genre_list'] = m.genres.apply(lambda x: [y['name'] for y in literal_eval(x)])
            print('start processing')
            df_1 = r.merge(m, on="movieid")
            df_1 = df_1[df_1['budget'].ne('0')]

            df_1['budget'] = df_1['budget'].astype("int64")
            df_1 = df_1[['movieid', 'budget', 'rating', 'genres','runtime']]

            print("Uploading processed table to ODS")
            upload_table(table_name="ods_movies_meta", dfr=df_1)
        else:
            print("ODS already fit")
    
    @task
    def process_ods_to_pdm(**kwargs):
        params = kwargs['params']
        df = get_table("ods_movies_meta")

        # 1 -- влияние жанра на рейтинг
        all_genres = []
        for genres in df.genres.apply(lambda x: [y['name'] for y in literal_eval(x)]).tolist():
            if genres is not None:
                all_genres.extend(genres)
        all_genres = sorted(list(set(all_genres)))

        res = {}
        for genre in all_genres:
            res[genre] = df[df['genres'].str.contains(genre)]['rating'].mean()
        res = {k: v for k, v in res.items() if not str(v).isalnum()}

        data = np.array(sorted([(k[0], k[1]) for k in res.items()], key=lambda x: x[1]))
        x = list(map(lambda x: str(x[0]), data[:,:1]))
        y = list(map(lambda x: float(x), data[:,1:]))
        df_1 = pd.DataFrame({"x": x, "y": y})
        upload_table("pdm_thesis_1", df_1) 

        # 2 -- влияние бюджета на рейтинг
        df_2 = df[['budget', 'rating']].copy()
        upload_table("pdm_thesis_2", df_2)

        # 3 -- влияние продолжительности на рейтинг
        df_3 = df[['runtime', 'rating']].copy()
        upload_table("pdm_thesis_3", df_3)
    
    @task
    def thesis_1(**kwargs):
        params = kwargs['params']
        path_to_folder = os.path.join(def_path, params['folder_path'])
        df = get_table("pdm_thesis_1")
        x = df['x']
        y = df['y']
        plt.xticks(rotation=60)
        plt.ylabel("rating")
        plt.title("rating from genre")
        plt.plot(x, y)
        plt.savefig(os.path.join(path_to_folder, "thesis_1.png"))
    
    @task
    def thesis_2(**kwargs):
        params = kwargs['params']
        path_to_folder = os.path.join(def_path, params['folder_path'])
        df = get_table("pdm_thesis_2")
        plt.title("rating from budget")
        plt.xlabel("rating")
        plt.ylabel("budget, milliard $")
        plt.plot(df[['rating', 'budget']].groupby("rating").mean())
        plt.savefig(os.path.join(path_to_folder, "thesis_2.png"))
    
    @task
    def thesis_3(**kwargs):
        params = kwargs['params']
        path_to_folder = os.path.join(def_path, params['folder_path'])
        df = get_table("pdm_thesis_3")
        df.set_index("runtime")
        plt.title("rating from duration")
        plt.ylabel("rating")
        plt.xlabel("duration, minutes")
        plt.plot(df[['rating', 'runtime']].groupby("runtime").mean())
        plt.savefig(os.path.join(path_to_folder, "thesis_3.png"))

    @task
    def collector(**kwargs):
        from PIL import Image

        params = kwargs['params']
        path_to_folder = os.path.join(def_path, params['folder_path'])

        images = [Image.open(os.path.join(path_to_folder, x)) for x in ['thesis_1.png', 'thesis_2.png', 'thesis_3.png']]
        widths, heights = zip(*(i.size for i in images))

        total_width = sum(widths)
        max_height = max(heights)

        new_im = Image.new('RGB', (total_width, max_height))

        x_offset = 0
        for im in images:
            new_im.paste(im, (x_offset,0))
            x_offset += im.size[0]

        new_im.save(os.path.join(path_to_folder, 'RESULT.jpg'))

    insert_tables_raw_to_dds() >> insert_tables_dds_to_ods() >> process_ods_to_pdm() >> [thesis_1(), thesis_2(), thesis_3()] >> collector()
