# search.py
import logging
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SimpleField,
    SearchableField,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
    SearchField,
)
from config import (
    SEARCH_SERVICE_ENDPOINT,
    SEARCH_SERVICE_KEY,
    OPENAI_EMBEDDING_MODEL
)
from tenacity import retry, stop_after_attempt, wait_exponential

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("VectorStore")

class VectorStore:
    def __init__(self, user_id):
        logger.info(f"Initializing VectorStore for user {user_id}")
        self.user_id = user_id
        self.credential = AzureKeyCredential(SEARCH_SERVICE_KEY)
        self.index_name = f"inventory-{user_id}"
        self.index_client = SearchIndexClient(
            endpoint=SEARCH_SERVICE_ENDPOINT,
            credential=self.credential
        )
        self.search_client = None
        
        # Check if index already exists
        try:
            if self.index_name in list(self.index_client.list_index_names()):
                logger.info(f"Found existing index: {self.index_name}")
                self._connect_to_index()
            else:
                logger.info(f"No existing index found for: {self.index_name}")
        except Exception as e:
            logger.error(f"Error checking index existence: {str(e)}")
    
    def _connect_to_index(self):
        """Helper method to connect to an existing index."""
        try:
            self.search_client = SearchClient(
                endpoint=SEARCH_SERVICE_ENDPOINT,
                credential=self.credential,
                index_name=self.index_name
            )
            logger.info(f"Connected to existing index: {self.index_name}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to index: {str(e)}")
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def create_index(self):
        """Create search index with retry logic."""
        try:
            # Try to delete existing index
            try:
                self.index_client.delete_index(self.index_name)
                logger.info(f"Deleted existing index: {self.index_name}")
            except Exception as e:
                logger.info(f"No existing index to delete: {self.index_name}")
            
            # Configure vector search
            vector_search = VectorSearch(
                algorithms=[
                    HnswAlgorithmConfiguration(
                        name="hnsw-config",
                        kind="hnsw",
                        parameters={
                            "m": 4,
                            "efConstruction": 400,
                            "efSearch": 500,
                            "metric": "cosine"
                        }
                    )
                ],
                profiles=[
                    VectorSearchProfile(
                        name="vector-profile",
                        algorithm_configuration_name="hnsw-config"
                    )
                ]
            )

            # Define fields with better naming and appropriate properties
            fields = [
                SimpleField(name="id", type="Edm.String", key=True),
                SimpleField(name="userId", type="Edm.String", filterable=True),
                SearchableField(name="supplier_name", type="Edm.String", filterable=True, searchable=True, sortable=True),
                SearchableField(name="inventory_item_name", type="Edm.String", filterable=True, searchable=True, sortable=True),
                SearchableField(name="item_name", type="Edm.String", filterable=True, searchable=True),
                SimpleField(name="item_number", type="Edm.String", filterable=True),
                SimpleField(name="quantity_in_case", type="Edm.Double", filterable=True, sortable=True),
                SimpleField(name="total_units", type="Edm.Double", filterable=True, sortable=True),
                SimpleField(name="case_price", type="Edm.Double", filterable=True, sortable=True),
                SimpleField(name="cost_of_unit", type="Edm.Double", filterable=True, sortable=True),
                SearchableField(name="category", type="Edm.String", filterable=True, searchable=True, sortable=True),
                SearchableField(name="measured_in", type="Edm.String", filterable=True),
                SimpleField(name="catch_weight", type="Edm.String", filterable=True),
                SearchableField(name="priced_by", type="Edm.String", filterable=True),
                SimpleField(name="splitable", type="Edm.String", filterable=True),
                SearchableField(name="content", type="Edm.String", searchable=True),
                SearchField(
                    name="content_vector",
                    type="Collection(Edm.Single)",
                    vector_search_dimensions=1536,  # Matches OpenAI's embedding dimensions
                    vector_search_profile_name="vector-profile"
                )
            ]

            logger.info(f"Creating new index with fields: {[f.name for f in fields]}")
            
            # Create the index
            index = SearchIndex(
                name=self.index_name,
                fields=fields,
                vector_search=vector_search
            )
            
            self.index_client.create_or_update_index(index)
            logger.info(f"Successfully created index: {self.index_name}")
            
            # Connect to the newly created index
            self._connect_to_index()
            return True
            
        except Exception as e:
            logger.error(f"Error creating index: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def add_documents(self, documents):
        """Add documents to the index with improved error handling and retries."""
        if not self.search_client:
            logger.error("Search client not initialized")
            await self.connect_to_index()
            if not self.search_client:
                raise ValueError("Failed to initialize search client")
        
        if not documents:
            logger.warning("No documents provided to add_documents")
            return []
            
        try:
            logger.info(f"Processing {len(documents)} documents for upload")
            
            # Validate documents first
            validated_docs = []
            for i, doc in enumerate(documents):
                try:
                    # Essential validation
                    if 'id' not in doc or not doc['id']:
                        logger.warning(f"Document {i} missing id field, generating a new one")
                        continue
                        
                    if 'content_vector' not in doc or not doc['content_vector']:
                        logger.warning(f"Document {i} missing content_vector, skipping")
                        continue
                    
                    # Check vector dimensions
                    vector_dim = len(doc['content_vector'])
                    if vector_dim != 1536:  # OpenAI embedding dimension
                        logger.warning(f"Document {i} has incorrect vector dimensions: {vector_dim}, skipping")
                        continue
                        
                    # Add to validated docs
                    validated_docs.append(doc)
                    
                except Exception as e:
                    logger.error(f"Error validating document {i}: {str(e)}")
                    continue
            
            if not validated_docs:
                logger.error("No valid documents to upload after validation")
                return []
                
            # Upload in batches to avoid request size limits
            batch_size = 100
            results = []
            
            for i in range(0, len(validated_docs), batch_size):
                batch = validated_docs[i:i+batch_size]
                logger.info(f"Uploading batch {(i//batch_size)+1} with {len(batch)} documents")
                
                try:
                    result = self.search_client.upload_documents(documents=batch)
                    results.append(result)
                    logger.info(f"Successfully uploaded batch {(i//batch_size)+1}")
                except Exception as e:
                    logger.error(f"Error uploading batch {(i//batch_size)+1}: {str(e)}")
                    # Continue with next batch rather than failing completely
                    
            logger.info(f"Document upload complete. Total batches: {len(results)}")
            return results
            
        except Exception as e:
            logger.error(f"Error in add_documents: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def search(self, query_vector, top_k=5, filter_condition=None):
        """Perform vector search with additional features and better error handling."""
        if not self.search_client:
            logger.error("Search client not initialized")
            await self.connect_to_index()
            if not self.search_client:
                raise ValueError("Failed to initialize search client")
                
        try:
            # Select fields to return in search results
            select_fields = [
                "inventory_item_name",
                "item_name",
                "category",
                "case_price",
                "cost_of_unit",
                "total_units",
                "measured_in",
                "priced_by",
                "content",
                "supplier_name"
            ]
            
            # Prepare search options
            search_params = {
                "search_text": None,
                "vector_queries": [{
                    'vector': query_vector,
                    'fields': 'content_vector',
                    'k': top_k,
                    'kind': 'vector'
                }],
                "select": ",".join(select_fields),
                "top": top_k
            }
            
            # Add filter if provided
            if filter_condition:
                search_params["filter"] = filter_condition
                
            # Execute search
            logger.info(f"Executing vector search with top_k={top_k}")
            results = self.search_client.search(**search_params)
            
            # Process results
            search_results = []
            for result in results:
                search_results.append(dict(result))
            
            logger.info(f"Search returned {len(search_results)} results")
            return search_results
            
        except Exception as e:
            logger.error(f"Error performing search: {str(e)}")
            raise

    async def connect_to_index(self):
        """Public method to connect to existing index with better error handling."""
        try:
            if not self.search_client:
                self.search_client = SearchClient(
                    endpoint=SEARCH_SERVICE_ENDPOINT,
                    credential=self.credential,
                    index_name=self.index_name
                )
                logger.info(f"Connected to existing index: {self.index_name}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to index: {str(e)}")
            raise
            
    async def delete_documents(self, document_ids):
        """Delete documents from the index."""
        if not self.search_client:
            logger.error("Search client not initialized")
            await self.connect_to_index()
        
        try:
            # Prepare documents for deletion
            docs_to_delete = [{"id": doc_id} for doc_id in document_ids]
            
            # Delete documents
            logger.info(f"Deleting {len(docs_to_delete)} documents")
            result = self.search_client.delete_documents(documents=docs_to_delete)
            
            logger.info(f"Documents deleted successfully")
            return result
        except Exception as e:
            logger.error(f"Error deleting documents: {str(e)}")
            raise