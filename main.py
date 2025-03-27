# main.py
import logging
import os
import uuid
import asyncio
import json
from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import time
from rag import RAGAssistant
from agent_tools import InventoryAgent
import uvicorn
from azure.search.documents.indexes import SearchIndexClient
from azure.core.credentials import AzureKeyCredential
from config import SEARCH_SERVICE_ENDPOINT, SEARCH_SERVICE_KEY

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("InventoryAPI")

app = FastAPI(
    title="Restaurant Inventory Assistant API",
    description="API for restaurant inventory management powered by AI",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Specify your frontend origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store RAG assistants in memory
rag_assistants: Dict[str, RAGAssistant] = {}

# Store Inventory Agents in memory
inventory_agents: Dict[str, InventoryAgent] = {}

# Request and response models
class Question(BaseModel):
    text: str
    user_id: str
    conversation_id: Optional[str] = None

class AgentAction(BaseModel):
    action: str
    user_id: str
    params: Dict[str, Any]
    conversation_id: Optional[str] = None

class Response(BaseModel):
    response: str
    conversation_id: str
    processing_time: float

class AgentResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    processing_time: float

class InitializeRequest(BaseModel):
    user_id: str
    force_rebuild: bool = False

class InitializeResponse(BaseModel):
    message: str
    status: str
    index_name: str

# Middleware for request timing
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Helper function to check if index exists
async def index_exists(user_id: str):
    try:
        index_name = f"inventory-{user_id}"
        index_client = SearchIndexClient(
            endpoint=SEARCH_SERVICE_ENDPOINT,
            credential=AzureKeyCredential(SEARCH_SERVICE_KEY)
        )
        indexes = list(index_client.list_index_names())
        return index_name in indexes
    except Exception as e:
        logger.error(f"Error checking index existence: {str(e)}")
        return False

# API health check
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

# Initialize user RAG system
@app.post("/initialize/{user_id}", response_model=InitializeResponse)
async def initialize_user_rag(user_id: str, request: InitializeRequest = None):
    try:
        force_rebuild = request.force_rebuild if request else False
        index_exists_flag = await index_exists(user_id)
        
        if index_exists_flag and user_id in rag_assistants and not force_rebuild:
            logger.info(f"RAG system already initialized for user {user_id}")
            return InitializeResponse(
                message=f"RAG system already initialized for user {user_id}",
                status="existing",
                index_name=f"inventory-{user_id}"
            )
        
        # Create new RAG assistant
        logger.info(f"Creating new RAG assistant for user {user_id}")
        rag_assistant = RAGAssistant(user_id)
        await rag_assistant.initialize()
        rag_assistants[user_id] = rag_assistant
        
        # Create new inventory agent
        logger.info(f"Creating new inventory agent for user {user_id}")
        inventory_agent = InventoryAgent(user_id)
        inventory_agents[user_id] = inventory_agent
        
        return InitializeResponse(
            message=f"RAG system initialized for user {user_id}",
            status="created",
            index_name=f"inventory-{user_id}"
        )
    except Exception as e:
        logger.error(f"Error initializing RAG system: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Background task for lazy initialization
async def lazy_initialize(user_id: str):
    try:
        if user_id not in rag_assistants:
            logger.info(f"Lazy initializing RAG system for user {user_id}")
            rag_assistant = RAGAssistant(user_id)
            if await index_exists(user_id):
                await rag_assistant.vector_store.connect_to_index()
            else:
                await rag_assistant.initialize()
            rag_assistants[user_id] = rag_assistant
            
            # Initialize inventory agent
            inventory_agent = InventoryAgent(user_id)
            inventory_agents[user_id] = inventory_agent
            
            logger.info(f"Lazy initialization complete for user {user_id}")
    except Exception as e:
        logger.error(f"Error in lazy initialization: {str(e)}")

async def detect_action_request(text):
    """
    Detect if the user's question is requesting an action that should be handled by the agent
    Returns a tuple of (is_action, action_type, action_parameters)
    """
    text_lower = text.lower()
    
    # Check for price update requests
    if any(phrase in text_lower for phrase in ["update price", "change price", "set price", "modify price"]):
        # Try to extract item number and new price
        import re
        
        # Extract item number - look for patterns like "item #123", "item number 123", etc.
        item_number_match = re.search(r'item\s+(?:number|#)?\s*(\w+[-\w]*)', text_lower)
        item_number = item_number_match.group(1) if item_number_match else None
        
        # Extract price - look for dollar amounts
        price_match = re.search(r'\$?(\d+\.?\d*)', text_lower)
        new_price = float(price_match.group(1)) if price_match else None
        
        # Determine price type (unit cost or case price)
        price_type = "Case Price" if "case price" in text_lower else "Cost of a Unit"
        
        if item_number and new_price:
            return True, "update_price", {"item_number": item_number, "new_price": new_price, "price_type": price_type}
    
    # Check for quantity update requests
    elif any(phrase in text_lower for phrase in ["update quantity", "change quantity", "set quantity", "modify quantity"]):
        import re
        
        # Extract item number
        item_number_match = re.search(r'item\s+(?:number|#)?\s*(\w+[-\w]*)', text_lower)
        item_number = item_number_match.group(1) if item_number_match else None
        
        # Extract quantity
        quantity_match = re.search(r'to\s+(\d+\.?\d*)', text_lower)
        new_quantity = float(quantity_match.group(1)) if quantity_match else None
        
        if item_number and new_quantity:
            return True, "update_quantity", {"item_number": item_number, "new_quantity": new_quantity}
    
    # Check for item detail requests
    elif any(phrase in text_lower for phrase in ["item details", "details for item", "information about item"]):
        import re
        
        # Extract item number
        item_number_match = re.search(r'item\s+(?:number|#)?\s*(\w+[-\w]*)', text_lower)
        item_number = item_number_match.group(1) if item_number_match else None
        
        if item_number:
            return True, "get_item_details", {"item_number": item_number}
    
    # Not an action request
    return False, None, {}

async def perform_agent_action(agent, action_type, params):
    """
    Perform the requested action using the inventory agent
    """
    try:
        result = {"success": False, "message": "Unknown action", "data": None}
        
        if action_type == "update_price":
            result = await agent.update_item_price(
                params["item_number"], 
                params["new_price"],
                params.get("price_type", "Cost of a Unit")
            )
        
        elif action_type == "update_quantity":
            result = await agent.update_item_quantity(
                params["item_number"],
                params["new_quantity"]
            )
        
        elif action_type == "get_item_details":
            result = await agent.get_item_details(params["item_number"])
            if result["success"] and "item" in result:
                result["data"] = result["item"]
                
        # Add additional action types as needed
        
        return result
    
    except Exception as e:
        logger.error(f"Error performing agent action: {str(e)}")
        return {"success": False, "message": f"Error performing action: {str(e)}", "data": None}

# Query endpoint
# Update the query endpoint to properly handle parameters from RAG assistant
@app.post("/query", response_model=Response)
async def query_rag(question: Question, background_tasks: BackgroundTasks):
    start_time = time.time()
    user_id = question.user_id
    conversation_id = question.conversation_id or str(uuid.uuid4())
    
    try:
        # Check if RAG assistant exists or needs initialization
        if user_id not in rag_assistants:
            # Add initialization as background task and use simpler response for now
            background_tasks.add_task(lazy_initialize, user_id)
            
            # Check if index exists
            if await index_exists(user_id):
                logger.info(f"Index exists for user {user_id}, connecting...")
                rag_assistant = RAGAssistant(user_id)
                await rag_assistant.vector_store.connect_to_index()
                rag_assistants[user_id] = rag_assistant
                
                # Initialize inventory agent
                inventory_agent = InventoryAgent(user_id)
                inventory_agents[user_id] = inventory_agent
            else:
                logger.info(f"No index found for user {user_id}")
                # Return a helpful message while initialization happens in background
                return Response(
                    response="I'm preparing your inventory data for the first time. Please ask your question again in a few moments.",
                    conversation_id=conversation_id,
                    processing_time=time.time() - start_time
                )
        
        # First check if the RAG assistant detects action intent
        rag_assistant = rag_assistants[user_id]
        has_action_intent, action_type, action_params = await rag_assistant.check_for_action_intent(question.text)
        
        # If action intent detected by RAG
        if has_action_intent:
            logger.info(f"Action intent detected by RAG: {action_type} with params: {action_params}")
            
            # Use the inventory agent to perform the action
            if user_id not in inventory_agents:
                inventory_agents[user_id] = InventoryAgent(user_id)
                
            agent = inventory_agents[user_id]
            
            # Map parameters correctly based on action type
            if action_type == "update_price":
                # Convert params to expected format
                mapped_params = {
                    "item_identifier": action_params.get("item_number") or action_params.get("item_identifier"),
                    "new_price": float(action_params.get("new_price")),
                    "price_type": action_params.get("price_type", "Cost of a Unit")
                }
                
                # Make sure price_type is valid
                if mapped_params["price_type"] not in ["Cost of a Unit", "Case Price"]:
                    mapped_params["price_type"] = "Cost of a Unit"
                
                logger.info(f"Performing price update with params: {mapped_params}")
                result = await agent.update_item_price(
                    mapped_params["item_identifier"],
                    mapped_params["new_price"],
                    mapped_params["price_type"]
                )
            
            elif action_type == "update_quantity":
                # Convert params to expected format
                mapped_params = {
                    "item_identifier": action_params.get("item_number") or action_params.get("item_identifier"),
                    "new_quantity": float(action_params.get("new_quantity"))
                }
                
                logger.info(f"Performing quantity update with params: {mapped_params}")
                result = await agent.update_item_quantity(
                    mapped_params["item_identifier"],
                    mapped_params["new_quantity"]
                )
            
            elif action_type == "get_item_details":
                # Convert params to expected format
                item_identifier = action_params.get("item_number") or action_params.get("item_identifier")
                
                logger.info(f"Getting item details for: {item_identifier}")
                result = await agent.get_item_details(item_identifier)
                if result["success"] and "item" in result:
                    result["data"] = result["item"]
            
            else:
                # Fall back to basic action detection for other types
                is_action_request, detected_action_type, detected_action_params = await detect_action_request(question.text)
                
                if is_action_request:
                    logger.info(f"Performing action detected via regex: {detected_action_type}")
                    result = await perform_agent_action(agent, detected_action_type, detected_action_params)
                else:
                    # If we get here, we received an action but couldn't process it
                    result = {
                        "success": False,
                        "message": f"Unable to process action: {action_type}. Missing required parameters."
                    }
            
            # Generate a natural language response based on the action result
            if result["success"]:
                response = f"✅ {result['message']}"
                if 'data' in result and result['data']:
                    response += "\n\nHere are the details:\n"
                    response += json.dumps(result['data'], indent=2)
            else:
                response = f"❌ {result['message']}"
            
            processing_time = time.time() - start_time
            logger.info(f"Processed agent action in {processing_time:.2f} seconds")
            
        else:
            # Fall back to traditional regex-based action detection if RAG didn't detect an action
            is_action_request, action_type, action_params = await detect_action_request(question.text)
            
            if is_action_request:
                # Use the inventory agent to perform the action
                if user_id not in inventory_agents:
                    inventory_agents[user_id] = InventoryAgent(user_id)
                    
                agent = inventory_agents[user_id]
                logger.info(f"Performing action detected via regex: {action_type} with params: {action_params}")
                result = await perform_agent_action(agent, action_type, action_params)
                
                # Generate a natural language response based on the action result
                if result["success"]:
                    response = f"✅ {result['message']}"
                    if 'data' in result and result['data']:
                        response += "\n\nHere are the details:\n"
                        response += json.dumps(result['data'], indent=2)
                else:
                    response = f"❌ {result['message']}"
                
                processing_time = time.time() - start_time
                logger.info(f"Processed agent action in {processing_time:.2f} seconds")
            else:
                # It's a regular query, not an action
                response = await rag_assistant.query(question.text)
                
                processing_time = time.time() - start_time
                logger.info(f"Processed query in {processing_time:.2f} seconds")
        
        return Response(
            response=response,
            conversation_id=conversation_id,
            processing_time=processing_time
        )
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        
        # Return a graceful error response
        return Response(
            response="I encountered an issue while processing your request. Please try again.",
            conversation_id=conversation_id,
            processing_time=time.time() - start_time
        )

# Explicit agent action endpoint
@app.post("/agent/action", response_model=AgentResponse)
async def agent_action(action_request: AgentAction):
    start_time = time.time()
    user_id = action_request.user_id
    
    try:
        # Ensure agent exists for this user
        if user_id not in inventory_agents:
            inventory_agents[user_id] = InventoryAgent(user_id)
        
        agent = inventory_agents[user_id]
        
        # Perform the requested action
        if action_request.action == "update_price":
            result = await agent.update_item_price(
                action_request.params.get("item_number"),
                action_request.params.get("new_price"),
                action_request.params.get("price_type", "Cost of a Unit")
            )
            
        elif action_request.action == "update_quantity":
            result = await agent.update_item_quantity(
                action_request.params.get("item_number"),
                action_request.params.get("new_quantity")
            )
            
        elif action_request.action == "add_item":
            result = await agent.add_new_inventory_item(
                action_request.params.get("item_data", {})
            )
            
        elif action_request.action == "delete_item":
            result = await agent.delete_inventory_item(
                action_request.params.get("item_number")
            )
            
        elif action_request.action == "search_by_category":
            result = await agent.search_items_by_category(
                action_request.params.get("category")
            )
            if result["success"] and "items" in result:
                result["data"] = {"items": result["items"]}
                del result["items"]
            
        elif action_request.action == "get_item_details":
            result = await agent.get_item_details(
                action_request.params.get("item_number")
            )
            if result["success"] and "item" in result:
                result["data"] = {"item": result["item"]}
                del result["item"]
                
        else:
            result = {
                "success": False,
                "message": f"Unknown action: {action_request.action}"
            }
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Prepare and return response
        return AgentResponse(
            success=result.get("success", False),
            message=result.get("message", "Action completed"),
            data=result.get("data"),
            processing_time=processing_time
        )
        
    except Exception as e:
        logger.error(f"Error performing agent action: {str(e)}")
        return AgentResponse(
            success=False,
            message=f"Error: {str(e)}",
            data=None,
            processing_time=time.time() - start_time
        )

# Refresh user index
@app.post("/refresh/{user_id}")
async def refresh_user_index(user_id: str):
    try:
        if user_id in rag_assistants:
            logger.info(f"Refreshing index for user {user_id}")
            await rag_assistants[user_id].index_user_documents()
            return {"message": f"Index refreshed for user {user_id}", "status": "success"}
        else:
            # Initialize if not exists
            logger.info(f"User {user_id} not initialized, creating new assistant")
            await initialize_user_rag(user_id)
            return {"message": f"Created new index for user {user_id}", "status": "created"}
    except Exception as e:
        logger.error(f"Error refreshing index: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Get index status
@app.get("/status/{user_id}")
async def get_index_status(user_id: str):
    try:
        index_exists_flag = await index_exists(user_id)
        assistant_loaded = user_id in rag_assistants
        agent_loaded = user_id in inventory_agents
        
        return {
            "user_id": user_id,
            "index_exists": index_exists_flag,
            "assistant_loaded": assistant_loaded,
            "agent_loaded": agent_loaded,
            "status": "ready" if index_exists_flag and assistant_loaded else "not_ready"
        }
    except Exception as e:
        logger.error(f"Error checking status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

## Get agent capabilities
@app.get("/agent/capabilities")
async def get_agent_capabilities():
    """Return the list of available agent actions and their descriptions"""
    return {
        "capabilities": [
            {
                "action": "update_price",
                "description": "Update the price of an inventory item",
                "parameters": ["item_identifier", "new_price", "price_type"],
                "notes": "item_identifier can be either item name or item number"
            },
            {
                "action": "update_quantity",
                "description": "Update the quantity of an inventory item",
                "parameters": ["item_identifier", "new_quantity"],
                "notes": "item_identifier can be either item name or item number"
            },
            {
                "action": "add_item",
                "description": "Add a new inventory item",
                "parameters": ["item_data"],
                "notes": "item_data should contain all required fields for a new inventory item"
            },
            {
                "action": "delete_item",
                "description": "Delete an inventory item",
                "parameters": ["item_identifier"],
                "notes": "item_identifier can be either item name or item number"
            },
            {
                "action": "search_by_category",
                "description": "Search for items by category",
                "parameters": ["category"],
                "notes": "Returns all items matching the specified category"
            },
            {
                "action": "get_item_details",
                "description": "Get detailed information about a specific inventory item",
                "parameters": ["item_identifier"],
                "notes": "item_identifier can be either item name or item number"
            }
        ]
    }

if __name__ == "__main__":
    # Get port from environment or use default
    port = int(os.environ.get("PORT", 8000))
    
    # Run the FastAPI app
    uvicorn.run(app, host="0.0.0.0", port=port)