from ingest import Ingest

SOURCE_NAME = "news_api"
DATASET = "top-headlines"

ingest = Ingest()

ingest.ingest_bronze(source_name=SOURCE_NAME, dataset=DATASET)