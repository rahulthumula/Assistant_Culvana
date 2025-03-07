# main.py
import logging
import os
import uuid
import asyncio
from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import time
from rag import RAGAssistant
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

# Request and response models
class Question(BaseModel):
    text: str
    user_id: str
    conversation_id: Optional[str] = None

class Response(BaseModel):
    response: str
    conversation_id: str
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
            logger.info(f"Lazy initialization complete for user {user_id}")
    except Exception as e:
        logger.error(f"Error in lazy initialization: {str(e)}")

# Query endpoint
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
            else:
                logger.info(f"No index found for user {user_id}")
                # Return a helpful message while initialization happens in background
                return Response(
                    response="I'm preparing your inventory data for the first time. Please ask your question again in a few moments.",
                    conversation_id=conversation_id,
                    processing_time=time.time() - start_time
                )
        
        # Get response from RAG assistant
        rag_assistant = rag_assistants[user_id]
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
        
        return {
            "user_id": user_id,
            "index_exists": index_exists_flag,
            "assistant_loaded": assistant_loaded,
            "status": "ready" if index_exists_flag and assistant_loaded else "not_ready"
        }
    except Exception as e:
        logger.error(f"Error checking status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Get port from environment or use default
    port = int(os.environ.get("PORT", 8000))
    
    # Run the FastAPI app
    uvicorn.run(app, host="0.0.0.0", port=port)