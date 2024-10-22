"""
## Tutorial DAG: Load and query video game descriptions with MongoDB and OpenAI
"""

import logging
import os

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pendulum import datetime

t_log = logging.getLogger("airflow.task")

_MONGO_DB_CONN = os.getenv("MONGO_DB_CONN", "mongodb_default")
_MONGO_DB_DATABASE_NAME = os.getenv("MONGO_DB_DATABASE_NAME", "games")
_MONGO_DB_COLLECTION_NAME = os.getenv("MONGO_DB_COLLECTION_NAME", "games_nostalgia")
_MONGO_DB_SEARCH_INDEX_NAME = os.getenv("MONGO_DB_SEARCH_INDEX_NAME", "find_me_a_game")
_MONGO_DB_VECTOR_COLUMN_NAME = os.getenv("MONGO_DB_VECTOR_COLUMN_NAME", "vector")

_OPENAI_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
_OPENAI_EMBEDDING_MODEL_DIMENSIONS = os.getenv(
    "OPENAI_EMBEDDING_MODEL_DIMENSIONS", 1536
)

_DATA_TEXT_FILE_PATH = os.getenv("DATA_TEXT_FILE_PATH", "include/games.txt")

_COLLECTION_EXISTS_TASK_ID = "collection_already_exists"
_CREATE_COLLECTION_TASK_ID = "create_collection"
_CREATE_INDEX_TASK_ID = "create_search_index"
_INDEX_EXISTS_TASK_ID = "search_index_already_exists"


def _get_mongodb_database(
    mongo_db_conn_id: str = _MONGO_DB_CONN,
    mongo_db_database_name: str = _MONGO_DB_DATABASE_NAME,
):
    """
    Get the MongoDB database.
    Args:
        mongo_db_conn_id (str): The connection ID for the MongoDB connection.
        mongo_db_database_name (str): The name of the database.
    Returns:
        The MongoDB database.
    """
    hook = MongoHook(mongo_conn_id=mongo_db_conn_id)
    client = hook.get_conn()
    return client[mongo_db_database_name]


def _create_openai_embeddings(text: str, model: str):
    """
    Create embeddings for a text with the OpenAI API.
    Args:
        text (str): The text to create embeddings for.
        model (str): The OpenAI model to use.
    Returns:
        The embeddings for the text.
    """
    from openai import OpenAI

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    response = client.embeddings.create(input=text, model=model)
    embeddings = response.data[0].embedding

    return embeddings


@dag(
    start_date=datetime(2024, 10, 1),
    schedule=None,
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["mongodb"],
    doc_md=__doc__,
    params={
        "game_concepts": Param(
            ["fantasy", "quests"],
            type="array",
            description=(
                "What kind of game do you want to play today?"
                + " Add one concept per line."
            ),
        ),
    },
)
def query_game_vectors():

    @task.branch
    def check_for_collection() -> str:
        "Check if the provided collection already exists and decide on the next step."
        database = _get_mongodb_database()
        collection_list = database.list_collection_names()
        if _MONGO_DB_COLLECTION_NAME in collection_list:
            return _COLLECTION_EXISTS_TASK_ID
        else:
            return _CREATE_COLLECTION_TASK_ID

    @task(task_id=_CREATE_COLLECTION_TASK_ID)
    def create_collection():
        "Create a new collection in the database."
        database = _get_mongodb_database()
        database.create_collection(_MONGO_DB_COLLECTION_NAME)

    collection_already_exists = EmptyOperator(task_id=_COLLECTION_EXISTS_TASK_ID)
    collection_ready = EmptyOperator(
        task_id="collection_ready", trigger_rule="none_failed"
    )

    @task
    def extract() -> list:
        """
        Extract the games from the text file.
        Returns:
            list: A list with the games.
        """
        import re

        with open(_DATA_TEXT_FILE_PATH, "r") as f:
            games = f.readlines()

        games_list = []

        for game in games:

            parts = game.split(":::")
            title_year = parts[1].strip()
            match = re.match(r"(.+) \((\d{4})\)", title_year)

            title, year = match.groups()
            year = int(year)

            genre = parts[2].strip()
            description = parts[3].strip()

            game_data = {
                "title": title,
                "year": year,
                "genre": genre,
                "description": description,
            }

            games_list.append(game_data)

        return games_list

    @task(map_index_template="{{ game_str }}")
    def transform_create_embeddings(game: dict) -> dict:
        """
        Create embeddings for the game description.
        Args:
            game (dict): A dictionary with the game's data.
        Returns:
            dict: The game's data with the embeddings.
        """
        embeddings = _create_openai_embeddings(
            text=game.get("description"), model=_OPENAI_EMBEDDING_MODEL
        )
        game[_MONGO_DB_VECTOR_COLUMN_NAME] = embeddings

        # optional: setting the custom map index
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["game_str"] = f"{game['title']} ({game['year']}) - {game['genre']}"

        return game

    @task(trigger_rule="none_failed", map_index_template="{{ game_str }}")
    def load_data_to_mongo_db(game_data: dict) -> None:
        """
        Load the game data to the MongoDB collection.
        Args:
            game_data (dict): A dictionary with the game's data.
        """

        database = _get_mongodb_database()
        collection = database[_MONGO_DB_COLLECTION_NAME]

        filter_query = {
            "title": game_data["title"],
            "year": game_data["year"],
            "genre": game_data["genre"],
        }

        game_str = f"{game_data['title']} ({game_data['year']}) - {game_data['genre']}"

        existing_document = collection.find_one(filter_query)

        if existing_document:
            if existing_document.get("description") != game_data["description"]:
                collection.update_one(
                    filter_query, {"$set": {"description": game_data["description"]}}
                )
                t_log.info(f"Updated description for record: {game_str}")
            else:
                t_log.info(f"Skipped duplicate record: {game_str}")
        else:
            collection.update_one(
                filter_query, {"$setOnInsert": game_data}, upsert=True
            )
            t_log.info(f"Inserted record: {game_str}")

        # optional: setting the custom map index
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["game_str"] = game_str

    @task.branch
    def check_for_search_index() -> str:
        "Check if the provided index already exists and decide on the next step."
        database = _get_mongodb_database()
        collection = database[_MONGO_DB_COLLECTION_NAME]
        index_list = collection.list_search_indexes().to_list()
        index_name_list = [index.get("name") for index in index_list]
        if _MONGO_DB_SEARCH_INDEX_NAME in index_name_list:
            return _INDEX_EXISTS_TASK_ID
        else:
            return _CREATE_INDEX_TASK_ID

    @task(task_id=_CREATE_INDEX_TASK_ID)
    def create_search_index():
        """
        Create a search index model for the MongoDB collection.
        """
        from pymongo.operations import SearchIndexModel

        database = _get_mongodb_database()
        collection = database[_MONGO_DB_COLLECTION_NAME]

        search_index_model = SearchIndexModel(
            definition={
                "mappings": {
                    "dynamic": True,
                    "fields": {
                        _MONGO_DB_VECTOR_COLUMN_NAME: {
                            "type": "knnVector",
                            "dimensions": _OPENAI_EMBEDDING_MODEL_DIMENSIONS,
                            "similarity": "cosine",
                        }
                    },
                },
            },
            name=_MONGO_DB_SEARCH_INDEX_NAME,
        )

        collection.create_search_index(model=search_index_model)

    search_index_already_exists = EmptyOperator(task_id=_INDEX_EXISTS_TASK_ID)

    @task.sensor(
        poke_interval=10, timeout=3600, mode="poke", trigger_rule="none_failed"
    )
    def wait_for_full_indexing():
        """
        Wait for the search index to be fully built.
        """
        from airflow.sensors.base import PokeReturnValue

        database = _get_mongodb_database()
        collection = database[_MONGO_DB_COLLECTION_NAME]

        index_list = collection.list_search_indexes().to_list()
        index = next(
            (
                index
                for index in index_list
                if index.get("name") == _MONGO_DB_SEARCH_INDEX_NAME
            ),
            None,
        )

        if index:
            status = index.get("status")
            if status == "READY":
                t_log.info(f"Search index is {status}. Ready to query.")
                condition_met = True
            elif status == "FAILED":
                raise ValueError("Search index failed to build.")
            else:
                t_log.info(
                    f"Search index is {status}. Waiting for indexing to complete."
                )
                condition_met = False
        else:
            raise ValueError("Search index not found.")

        return PokeReturnValue(is_done=condition_met)

    @task
    def embed_concepts(**context):
        """
        Create embeddings for the provided concepts.
        """
        from openai import OpenAI

        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        game_concepts = context["params"]["game_concepts"]
        game_concepts_str = " ".join(game_concepts)

        embeddings = client.embeddings.create(
            input=game_concepts_str, model=_OPENAI_EMBEDDING_MODEL
        )

        return embeddings.to_dict()

    @task
    def query(query_vector: list):
        """
        Query the MongoDB collection for games based on the provided concepts.
        """

        db = _get_mongodb_database()
        collection = db[_MONGO_DB_COLLECTION_NAME]

        results = collection.aggregate(
            [
                {
                    "$vectorSearch": {
                        "exact": True,
                        "index": _MONGO_DB_SEARCH_INDEX_NAME,
                        "limit": 1,
                        "path": _MONGO_DB_VECTOR_COLUMN_NAME,
                        "queryVector": query_vector["data"][0]["embedding"],
                    }
                }
            ]
        )

        results_list = []

        for result in results:

            game_id = str(result["_id"])
            title = result["title"]
            year = result["year"]
            genre = result["genre"]
            description = result["description"]

            t_log.info(f"You should play {title}!")
            t_log.info(f"It was released in {year} and belongs to the {genre} genre.")
            t_log.info(f"Description: {description}")

            results_list.append(
                {
                    "game_id": game_id,
                    "title": title,
                    "year": year,
                    "genre": genre,
                    "description": description,
                }
            )

        return results_list

    _extract = extract()
    _transform_create_embeddings = transform_create_embeddings.expand(game=_extract)
    _load_data_to_mongo_db = load_data_to_mongo_db.expand(
        game_data=_transform_create_embeddings
    )

    _query = query(embed_concepts())

    chain(
        check_for_collection(),
        [create_collection(), collection_already_exists],
        collection_ready,
    )

    chain(
        collection_ready,
        check_for_search_index(),
        [create_search_index(), search_index_already_exists],
        wait_for_full_indexing(),
        _query,
    )

    chain(collection_ready, _load_data_to_mongo_db, _query)


query_game_vectors()