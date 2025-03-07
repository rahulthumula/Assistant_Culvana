# rag.py
from database import CosmosDB
from embeddings import EmbeddingGenerator
from search import VectorStore
from openai import OpenAI
from config import OPENAI_API_KEY, OPENAI_MODEL
import uuid
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RAGAssistant")

class RAGAssistant:
    def __init__(self, user_id):
        logger.info(f"Initializing RAGAssistant for user {user_id}")
        self.user_id = user_id
        self.cosmos_db = CosmosDB()
        self.embedding_generator = EmbeddingGenerator()
        self.vector_store = VectorStore(user_id)
        self.openai_client = OpenAI(api_key=OPENAI_API_KEY)
        
    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ValueError, ConnectionError))
    )
    async def _generate_embedding_with_retry(self, text):
        """Generate embedding with improved retry logic."""
        try:
            logger.info(f"Generating embedding for text (length: {len(text)})")
            embedding = await self.embedding_generator.generate_embedding(text)
            logger.info(f"Generated embedding dimensions: {len(embedding)}")
            return embedding
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}")
            raise

    def _create_item_content(self, item):
        """Create rich, searchable content for an inventory item with improved structure."""
        try:
            logger.info(f"Creating content for item: {item.get('Inventory Item Name', 'Unknown')}")
            
            # Ensure all fields have default values to prevent KeyErrors
            item_name = item.get('Inventory Item Name', 'Unknown Item')
            category = item.get('Category', 'unknown').lower()
            brand = item.get('Brand', '')
            full_name = item.get('Item Name', 'Unknown')
            case_price = item.get('Case Price', 0)
            unit_cost = item.get('Cost of a Unit', 0)
            priced_by = item.get('Priced By', 'unit').replace('per ', '')
            qty_in_case = item.get('Quantity In a Case', 0)
            measured_in = item.get('Measured In', 'units')
            total_units = item.get('Total Units', 0)
            item_number = item.get('Item Number', 'unknown')
            splitable = item.get('Splitable', 'NO')
            
            # Building a more structured content with clear sections
            sections = {
                "Product Overview": f"This is {item_name}, a {category} product{f' from {brand}' if brand else ''}. The full product name is {full_name}.",
                
                "Pricing Details": f"It costs ${case_price} per {priced_by}. Each unit costs ${unit_cost}.",
                
                "Quantity Information": f"Each case contains {qty_in_case} {measured_in}. Total available units are {total_units}.",
                
                "Specifications": f"The item number is {item_number}. {'This item cannot be split.' if splitable == 'NO' else 'This item can be split.'}"
            }
            
            # Add category-specific details
            if category.upper() == "DAIRY":
                sections["Storage Requirements"] = "This is a dairy product that should be stored refrigerated."
            elif category.upper() == "FROZEN":
                sections["Storage Requirements"] = "This is a frozen product that must be kept frozen."
            elif category.upper() == "PRODUCE":
                sections["Storage Requirements"] = "This is a fresh produce item with limited shelf life."
                
            # Assemble the final content with clear section formatting
            content = "\n\n".join([f"{key}:\n{value}" for key, value in sections.items()])
            
            logger.info(f"Created content successfully for {item_name}")
            return content
            
        except Exception as e:
            logger.error(f"Error creating content: {str(e)}")
            # Return a minimal content to avoid complete failure
            return f"Item: {item.get('Inventory Item Name', 'Unknown Item')}"

    async def index_inventory_items(self, inventory_list):
        """Process and index inventory items with improved error handling."""
        vector_documents = []
        logger.info(f"Processing {len(inventory_list)} inventory documents")
        
        if not inventory_list:
            logger.error("No inventory documents found")
            raise ValueError("No inventory documents found")
            
        inventory_doc = inventory_list[0]
        items = inventory_doc.get('items', [])
        
        if not items:
            logger.warning("Inventory document contains no items")
            return
        
        logger.info(f"Processing {len(items)} individual inventory items")
        
        for i, item in enumerate(items):
            try:
                # Create rich content
                content = self._create_item_content(item)
                
                # Generate embedding
                embedding = await self._generate_embedding_with_retry(content)
                
                # Create document with correct field mapping
                vector_doc = {
                    'id': str(uuid.uuid4()),
                    'userId': self.user_id,
                    'supplier_name': item.get('Supplier Name', ''),
                    'inventory_item_name': item.get('Inventory Item Name', ''),
                    'item_name': item.get('Item Name', ''),
                    'item_number': item.get('Item Number', ''),
                    'quantity_in_case': float(item.get('Quantity In a Case', 0)),
                    'total_units': float(item.get('Total Units', 0)),
                    'case_price': float(item.get('Case Price', 0)),
                    'cost_of_unit': float(item.get('Cost of a Unit', 0)),
                    'category': item.get('Category', ''),
                    'measured_in': item.get('Measured In', ''),
                    'catch_weight': item.get('Catch Weight', ''),
                    'priced_by': item.get('Priced By', ''),
                    'splitable': item.get('Splitable', ''),
                    'content': content,
                    'content_vector': embedding
                }
                
                vector_documents.append(vector_doc)
                logger.info(f"Successfully processed item {i+1}/{len(items)}: {item.get('Inventory Item Name')}")
                
            except Exception as e:
                logger.error(f"Error processing item {i}: {str(e)}")
                # Continue with next item instead of failing completely
                continue
        
        if vector_documents:
            logger.info(f"Adding {len(vector_documents)} documents to vector store")
            await self.vector_store.add_documents(vector_documents)
        else:
            logger.warning("No documents were successfully processed for indexing")

    async def initialize(self):
        """Initialize the RAG system with better error handling and logging."""
        try:
            # Get user inventory
            logger.info(f"Fetching inventory data for user {self.user_id}")
            inventory = await self.cosmos_db.get_user_documents(self.user_id)
            
            if not inventory:
                logger.error(f"No inventory found for user {self.user_id}")
                raise ValueError(f"No inventory found for user {self.user_id}")
            
            logger.info(f"Retrieved {len(inventory)} inventory documents")
            
            # Create or update search index
            logger.info("Creating/updating search index")
            await self.vector_store.create_index()
            
            # Index inventory items
            logger.info("Indexing inventory items")
            await self.index_inventory_items(inventory)
            
            logger.info("Initialization completed successfully")
            
        except Exception as e:
            logger.error(f"Error during initialization: {str(e)}")
            raise

    async def query(self, user_question, top_k=5):
        """Improved query processing with clearer prompt structure and error handling."""
        try:
            logger.info(f"Processing query: '{user_question}'")
            
            # Generate embedding for the question
            question_embedding = await self._generate_embedding_with_retry(user_question)
            
            # Search for relevant inventory items
            logger.info(f"Searching for top {top_k} relevant items")
            search_results = await self.vector_store.search(question_embedding, top_k)
            
            if not search_results:
                logger.warning("No relevant inventory items found")
                return "I couldn't find any relevant inventory information to answer your question. Please try rephrasing or ask about specific inventory items."
            
            # Format the search results for the prompt
            formatted_results = self._format_search_results(search_results)
            
            # Construct a better prompt with clear sections
            prompt = self._construct_prompt(user_question, formatted_results)
            
            # Generate response
            logger.info("Generating response with fine-tuned model")
            response = self.openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a helpful restaurant inventory assistant that provides accurate information about inventory items, prices, and quantities. Answer questions based only on the inventory data provided. If the data doesn't contain the information needed, acknowledge that limitation. Format your response in a clear, professional manner."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,  # Lower temperature for more consistent responses
                max_tokens=15000
            )
            
            logger.info("Response generated successfully")
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            # Provide a graceful error message to the user
            return f"I encountered an issue while processing your question. Please try again or contact support if the problem persists."
    
    def _format_search_results(self, search_results):
        """Format search results in a clear, structured way for the prompt."""
        formatted_items = []
        
        for i, item in enumerate(search_results):
            # Extract key information
            inventory_item_name = item.get('inventory_item_name', 'Unknown')
            category = item.get('category', 'Unknown')
            cost = item.get('cost_of_unit', 0)
            total_units = item.get('total_units', 0)
            case_price = item.get('case_price', 0)
            
            # Format as structured data
            formatted_item = (
                f"Item {i+1}: {inventory_item_name}\n"
                f"  Category: {category}\n"
                f"  Unit Cost: ${cost}\n"
                f"  Total Units Available: {total_units}\n"
                f"  Case Price: ${case_price}\n"
                f"  Details: {item.get('content', '')}"
            )
            
            formatted_items.append(formatted_item)
        
        return "\n\n".join(formatted_items)
    
    def _construct_prompt(self, question, formatted_results):
        """Construct a clear prompt with explicit instructions."""
        prompt = f"""
I need information from my restaurant inventory to answer this question:

QUESTION:
{question}

RELEVANT INVENTORY DATA:
{formatted_results}

Based ONLY on the inventory data above, please provide a detailed answer to my question.
Format your response using HTML tags for better rendering in a chat interface.

For each inventory item, use this exact format:
<div style="margin-bottom: 20px; padding: 15px; background-color: #f8fafc; border-radius: 8px; border-left: 4px solid #3b82f6;">
  <h3 style="color: #334155; margin-top: 0; margin-bottom: 10px; font-size: 18px;">X. [Item Name]</h3>
  <ul style="list-style-type: none; padding-left: 10px; margin: 0;">
    <li style="margin-bottom: 8px;"><strong style="color: #475569;">Category:</strong> [Category]</li>
    <li style="margin-bottom: 8px;"><strong style="color: #475569;">Unit Cost:</strong> $[Cost]</li>
    <li style="margin-bottom: 8px;"><strong style="color: #475569;">Total Units Available:</strong> [Quantity]</li>
    <li style="margin-bottom: 8px;"><strong style="color: #475569;">Item Number:</strong> [Item Number]</li>
    <li style="margin-bottom: 0;"><strong style="color: #475569;">Notes:</strong> [Any additional notes]</li>
  </ul>
</div>

Then include a section called "Practical Insights:" with this format:
<div style="margin-top: 30px; background-color: #eff6ff; padding: 20px; border-radius: 8px;">
  <h3 style="color: #1e40af; margin-top: 0; margin-bottom: 15px; font-size: 20px;">Practical Insights:</h3>
  <div style="margin-bottom: 15px;">
    <p style="margin-top: 0; margin-bottom: 5px;"><strong style="color: #334155;">Key Insight 1:</strong> [Insight details]</p>
  </div>
  <div style="margin-bottom: 15px;">
    <p style="margin-top: 0; margin-bottom: 5px;"><strong style="color: #334155;">Key Insight 2:</strong> [Insight details]</p>
  </div>
  <!-- Additional insights as needed -->
</div>

Do not use Markdown formatting like ** or -. Use only HTML as specified above.
"""
        return prompt

    async def index_user_documents(self):
        """Re-index user documents (for refreshing the index)."""
        try:
            logger.info(f"Re-indexing documents for user {self.user_id}")
            
            # Get updated inventory
            inventory = await self.cosmos_db.get_user_documents(self.user_id)
            
            if not inventory:
                logger.error(f"No inventory found for user {self.user_id}")
                raise ValueError(f"No inventory found for user {self.user_id}")
            
            # Delete and recreate index
            logger.info("Recreating search index")
            await self.vector_store.create_index()
            
            # Index inventory items
            logger.info("Indexing updated inventory items")
            await self.index_inventory_items(inventory)
            
            logger.info("Re-indexing completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error during re-indexing: {str(e)}")
            raise