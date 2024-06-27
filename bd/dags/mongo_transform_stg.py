import time
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from sqlalchemy import create_engine, MetaData, select, insert, update, delete
from sqlalchemy.orm import sessionmaker
import json

# Настройки подключения
MONGO_URI = Variable.get('MONGO_URI')
DATABASE_URL_DWH = Variable.get('POSTGRESQL_URI')

def connect_to_mongo(retries=5, delay=5):
    for i in range(retries):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, socketTimeoutMS=20000)
            client.admin.command('ping')  # Проверка подключения
            return client
        except ConnectionFailure as e:
            print(f"Connection failed, retrying in {delay} seconds... (Attempt {i + 1}/{retries})")
            time.sleep(delay)
    raise Exception("Failed to connect to MongoDB after several retries")

def get_last_version(dwh_session, settings_table, stg_table_name):
    query = select([settings_table.c.settings]).where(
        settings_table.c.setting_key == f'{stg_table_name}_last_version'
    )
    result = dwh_session.execute(query).fetchone()
    if result:
        return result['settings']['last_version']
    else:
        return 0

def update_last_version(dwh_session, settings_table, stg_table_name, last_version):
    update_settings = update(settings_table).where(
        settings_table.c.setting_key == f'{stg_table_name}_last_version'
    ).values(
        settings={'last_version': last_version}
    )
    dwh_session.execute(update_settings)
    dwh_session.commit()

def insert_initial_settings(dwh_session, settings_table, stg_table_name, last_version):
    initial_settings = insert(settings_table).values(
        setting_key=f'{stg_table_name}_last_version',
        settings={'last_version': last_version}
    )
    dwh_session.execute(initial_settings)
    dwh_session.commit()

def load_data_from_mongo_to_stg(collection_name, stg_table_name, dwh_session, dwh_metadata):
    mongo_client = connect_to_mongo()
    mongo_db = mongo_client['mydatabase']
    stg_table = dwh_metadata.tables[f'stg.{stg_table_name}']
    settings_table = dwh_metadata.tables['stg.settings']

    last_version = get_last_version(dwh_session, settings_table, stg_table_name)
    if last_version == 0:
        insert_initial_settings(dwh_session, settings_table, stg_table_name, last_version)

    collection = mongo_db[collection_name]
    new_data = collection.find()

    for doc in new_data:
        doc_id = str(doc['_id'])
        doc.pop('_id')
        doc_json = json.dumps(doc, default=str, ensure_ascii=False)

        existing_record = dwh_session.execute(
            select([stg_table]).where(stg_table.c.obj_id == doc_id)
        ).fetchone()

        if existing_record:
            update_stmt = update(stg_table).where(stg_table.c.obj_id == doc_id).values(
                obj_val=doc_json,
                when_updated=pendulum.now(),
                version=last_version + 1
            )
            dwh_session.execute(update_stmt)
        else:
            insert_stmt = insert(stg_table).values(
                obj_id=f'{doc_id}',
                obj_val=doc_json,
                when_updated=pendulum.now(),
                version=last_version + 1
            )
            dwh_session.execute(insert_stmt)

    delete_stmt = delete(stg_table).where(stg_table.c.version <= last_version)
    dwh_session.execute(delete_stmt)

    update_last_version(dwh_session, settings_table, stg_table_name, last_version + 1)

    dwh_session.commit()
    dwh_session.close()

@dag(
    schedule_interval='@once',
    start_date=pendulum.now(),
    catchup=False,
    tags=['mongo_to_stg'],
    is_paused_upon_creation=False
)
def mongo_transform_stg():
    dwh_engine = create_engine(DATABASE_URL_DWH)
    DwhSession = sessionmaker(bind=dwh_engine)
    dwh_metadata = MetaData()
    dwh_metadata.reflect(bind=dwh_engine, schema='stg')

    @task()
    def load_clients():
        dwh_session = DwhSession()
        load_data_from_mongo_to_stg('Clients', 'mongo_clients', dwh_session, dwh_metadata)

    @task()
    def load_restaurants():
        dwh_session = DwhSession()
        load_data_from_mongo_to_stg('Restaurant', 'mongo_restaurants', dwh_session, dwh_metadata)

    @task()
    def load_orders():
        dwh_session = DwhSession()
        load_data_from_mongo_to_stg('Orders', 'mongo_orders', dwh_session, dwh_metadata)

    clients_task = load_clients()
    restaurants_task = load_restaurants()
    orders_task = load_orders()

    [clients_task, restaurants_task, orders_task]

mongo_to_stg_dag = mongo_transform_stg()
