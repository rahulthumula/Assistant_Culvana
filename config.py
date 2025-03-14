# config.py
from dotenv import load_dotenv
import os

load_dotenv()

# Azure Cosmos DB configuration
COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE")
COSMOS_CONTAINER = os.getenv("COSMOS_CONTAINER")

# OpenAI configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = "ft:gpt-4o-2024-08-06:culvana::B4wUeDCH"  # or your preferred model
OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"
SEARCH_MODEL="gpt-4o-search-preview"

# Azure Cognitive Search configuration
SEARCH_SERVICE_ENDPOINT = os.getenv("SEARCH_SERVICE_ENDPOINT")
SEARCH_SERVICE_KEY = os.getenv("SEARCH_SERVICE_KEY")
SEARCH_INDEX_NAME = os.getenv("SEARCH_INDEX_NAME")