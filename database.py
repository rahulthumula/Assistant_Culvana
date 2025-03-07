# database.py
from azure.cosmos import CosmosClient, PartitionKey
from config import (
    COSMOS_ENDPOINT,
    COSMOS_KEY,
    COSMOS_DATABASE,
    COSMOS_CONTAINER
)

class CosmosDB:
    def __init__(self):
        self.client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
        self.database = self.client.get_database_client(COSMOS_DATABASE)
        self.container = self.database.get_container_client(COSMOS_CONTAINER)

    async def get_user_documents(self, user_id):
        # Query documents for specific user
        query = f"SELECT * FROM c WHERE c.userId = '{user_id}'"
        items = list(self.container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        return items

    async def get_user_info(self, user_id):
        # Get specific user details
        query = f"SELECT * FROM c WHERE c.id = '{user_id}'"
        items = list(self.container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        return items[0] if items else None