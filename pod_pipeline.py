

from airflow.decorators import dag, task 
from datetime import datetime, timedelta 
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pendulum 
import requests
import xmltodict 
import os 

# adding connections to sqlite database 
# airflow connections add 'podcasts' --conn-type 'sqlite' --conn-host '/home/sasidhar/airflow/dags/episodes.db'
# aiflow connections get podcasts
default_args = { 
    'owner' : 'Sasidhar', 
    # 'retries' : 1, 
    # 'retry_delay' : timedelta(minutes=1) 

}

dag_args = { 
    "dag_id" : "podcast_summary2", 
    "default_args" : default_args,
    "description" : "podcasts", 
    "start_date" : pendulum.datetime(2023,3,15), 
    "schedule_interval" : '@daily',
    "catchup" : False 
}


@dag(**dag_args) 
def podcast_summary2() : 

    create_database = SqliteOperator(
        task_id = "create_table_sqlite", 
        sql = """
                CREATE TABLE IF NOT EXISTS episodes(
                    link Text primary key, 
                    title TEXT, 
                    filename TEXT, 
                    published TEXT, 
                    description TEXT 
                )
         """  , 
         sqlite_conn_id = "podcasts" 
    )
    
    @task() 
    def get_episodes() : 
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace")
        feed = xmltodict.parse(data.text) 
        episodes = feed['rss']['channel']['item'] 
        print(f'found {len(episodes)} episodes. ')
        return episodes 

    podcast_episodes = get_episodes() 
    create_database.set_downstream(podcast_episodes)


    @task() 
    def load_episodes(episodes) : 
        hook = SqliteHook(sqlite_conn_id = "podcasts")

        #query our episodes database and figure out
        # we already stored any of the episodes 
        
        stored = hook.get_pandas_df("SELECT * from episodes;") 
        new_episodes = [] 

        for episode in episodes : 
            if episode["link"] not in stored["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])

        hook.insert_rows(table="episodes", rows=new_episodes, target_fields = ["link", "title", "published", "description", "filename"]) 

    load_episodes(podcast_episodes)

    @task() 
    def download_episodes(episodes) : 
        for episode in episodes : 
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            audio_path = os.path.join('/home/sasidhar/airflow/dags/episodes/', filename) 

            if not os.path.exists(audio_path) : 
                print(f"downloading {filename}") 
                print(f"{audio_path}")
                audio = requests.get(episode["enclosure"]["@url"]) 
                
                with open(audio_path, "wb+") as f : 
                    f.write(audio.content) 

    download_episodes(podcast_episodes) 

summary = podcast_summary2() 

