## Orchestrate MongoDB operations with Apache Airflow

This repository contains the example DAG described in the [Orchestrate MongoDB operations with Apache Airflow](https://www.astronomer.io/docs/learn/airflow-mongodb) tutorial.
It ingest descriptions of video games from a local text file, creates vector embeddings using OpenAI and then loads the embedded data into a MongoDB collection. Additionally the DAG will set up the collection and search index if they do not already exist. The DAG also contains a task to query the data for specific concepts.

## Content

This repository contains:

- `query_game_vectors`: A DAG showing how to ingest text data as vector embeddings into MongoDB and query the data.
- `test_conn`: A DAG to test the connection to MongoDB.

## How to run the demo

1. Fork and clone this repository.
2. Make sure you have the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed and that [Docker](https://www.docker.com/) is running.
3. Copy the `.env.example` file to a new file called `.env` and fill in your own values and credentials into the `<>` placeholders. You will need a running [MongoDB cluster](https://www.mongodb.com/) as well as an [OpenAI API key](https://platform.openai.com/docs/overview) of at least tier 1.
4. Run `astro dev start` to start the Airflow instance. The webserver with the Airflow UI will be available at `localhost:8080`. Log in with the credentials `admin:admin`.
5. Run the `test_conn` DAG to test your connection to MongoDB. 
6. Run the `query_game_vectors` DAG to ingest text data as vector embeddings into MongoDB and query the data. When running the DAG you can provide concepts to query the data for.