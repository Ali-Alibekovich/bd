import requests
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy import create_engine, MetaData, select, insert, update, delete
from sqlalchemy.orm import sessionmaker
import json


DATABASE_URL_DWH = Variable.get('POSTGRESQL_URI')
API_NGROK_URL = Variable.get('API_NGROK_URL')


def create_dwh_session():
    engine = create_engine(DATABASE_URL_DWH)
    Session = sessionmaker(bind=engine)
    metadata = MetaData()
    metadata.reflect(bind=engine, schema='stg')
    return Session(), metadata


def fetch_data_from_api(url, params=None):
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")


def get_deliverers():
    url = f'{API_NGROK_URL}/api/delivery_man'
    deliverers = []
    data = fetch_data_from_api(url)
    deliverers.extend(data)
    return deliverers


def get_deliveryman_data(deliveryman_id):
    url = f'{API_NGROK_URL}/api/delivery/{deliveryman_id}'
    return fetch_data_from_api(url)


def get_last_version(dwh_session, settings_table, key):
    query = select([settings_table.c.settings]).where(settings_table.c.setting_key == key)
    result = dwh_session.execute(query).fetchone()
    return result['settings']['last_version'] if result else 0


def update_last_version(dwh_session, settings_table, key, version):
    update_settings = update(settings_table).where(settings_table.c.setting_key == key).values(
        settings={'last_version': version})
    dwh_session.execute(update_settings)
    dwh_session.commit()


def upsert_data(dwh_session, stg_table, obj_id, obj_val, version):
    existing_record = dwh_session.execute(select([stg_table]).where(stg_table.c.obj_id == obj_id)).fetchone()
    if existing_record:
        update_stmt = update(stg_table).where(stg_table.c.obj_id == obj_id).values(
            obj_val=obj_val, when_updated=pendulum.now(), version=version)
        dwh_session.execute(update_stmt)
    else:
        insert_stmt = insert(stg_table).values(
            obj_id=obj_id, obj_val=obj_val, when_updated=pendulum.now(), version=version)
        dwh_session.execute(insert_stmt)


def delete_old_versions(dwh_session, stg_table, version):
    delete_stmt = delete(stg_table).where(stg_table.c.version <= version)
    dwh_session.execute(delete_stmt)


@dag(
    schedule_interval='@once',
    start_date=pendulum.now(),
    catchup=False,
    tags=['api_to_stg'],
    is_paused_upon_creation=False
)
def api_transform_stg():

    @task
    def delivery_man():
        deliverers = get_deliverers()
        dwh_session, dwh_metadata = create_dwh_session()
        settings_table = dwh_metadata.tables['stg.settings']
        stg_table = dwh_metadata.tables['stg.api_deliveryman']

        last_version = get_last_version(dwh_session, settings_table, 'api_deliveryman_last_version')

        for deliverer in deliverers:
            deliverer_id = deliverer['id']
            deliverer_name = deliverer['name']
            upsert_data(dwh_session, stg_table, deliverer_id, deliverer_name, last_version + 1)

        delete_old_versions(dwh_session, stg_table, last_version)
        update_last_version(dwh_session, settings_table, 'api_deliveryman_last_version', last_version + 1)

        dwh_session.close()
        return deliverers

    @task
    def delivery(deliverers):
        dwh_session, dwh_metadata = create_dwh_session()
        settings_table = dwh_metadata.tables['stg.settings']
        stg_table = dwh_metadata.tables['stg.api_delivery']

        for deliverer in deliverers:
            deliverer_id = deliverer['id']
            deliveries = get_deliveryman_data(deliverer_id)
            last_version = get_last_version(dwh_session, settings_table, f'api_delivery_{deliverer_id}_last_version')

            for delivery in deliveries:
                delivery_id = delivery['deliveryId']
                delivery_json = json.dumps(delivery, default=str, ensure_ascii=False)
                upsert_data(dwh_session, stg_table, delivery_id, delivery_json, last_version + 1)

            delete_old_versions(dwh_session, stg_table, last_version)
            update_last_version(dwh_session, settings_table, f'api_delivery_{deliverer_id}_last_version',
                                last_version + 1)

        dwh_session.close()

    deliverers = delivery_man()
    delivery(deliverers)


api_to_stg_dag = api_transform_stg()

