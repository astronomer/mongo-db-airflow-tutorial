from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
)
def test_conn():

    @task
    def test_conn():
        hook = MongoHook(mongo_conn_id="mongo_db_json")
        client = hook.get_conn()
        print(f"Connected to MongoDB - {client.server_info()}")

    test_conn()


test_conn()
