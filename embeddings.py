# embeddings.py
import logging
from openai import OpenAI
from config import (
    OPENAI_API_KEY,
    OPENAI_EMBEDDING_MODEL
)
import numpy as np
from tenacity import retry, stop_after_attempt, wait_exponential

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EmbeddingGenerator")

class EmbeddingGenerator:
    # Expected dimensions for different OpenAI embedding models
    EXPECTED_DIMENSIONS = {
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072, 
        "text-embedding-ada-002": 1536  # Legacy model
    }

    def __init__(self):
        logger.info(f"Initializing EmbeddingGenerator with model: {OPENAI_EMBEDDING_MODEL}")
        
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        
        if OPENAI_EMBEDDING_MODEL not in self.EXPECTED_DIMENSIONS:
            error_msg = (
                f"Unsupported embedding model: {OPENAI_EMBEDDING_MODEL}. "
                f"Supported models are: {list(self.EXPECTED_DIMENSIONS.keys())}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.expected_dim = self.EXPECTED_DIMENSIONS[OPENAI_EMBEDDING_MODEL]
        logger.info(f"Expected dimensions for {OPENAI_EMBEDDING_MODEL}: {self.expected_dim}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def generate_embedding(self, text):
        """Generate embedding with improved error handling and retries."""
        if not text:
            logger.error("Cannot generate embedding for empty text")
            raise ValueError("Text cannot be empty")
            
        if not isinstance(text, str):
            logger.error(f"Expected string input, got {type(text)}")
            raise TypeError(f"Expected string input, got {type(text)}")
            
        # Truncate text if it's too long (OpenAI API has token limits)
        if len(text) > 8000:
            logger.warning(f"Text too long ({len(text)} chars), truncating to 8000 chars")
            text = text[:8000]
        
        try:
            logger.info(f"Generating embedding for text (length: {len(text)})")
            
            # Generate embedding
            response = self.client.embeddings.create(
                input=text,
                model=OPENAI_EMBEDDING_MODEL,
                encoding_format="float"
            )
            
            # Extract embedding
            embedding = response.data[0].embedding
            actual_dim = len(embedding)
            
            logger.info(f"Generated embedding with dimensions: {actual_dim}")
            
            # Validate embedding dimensions
            if actual_dim != self.expected_dim:
                error_msg = (
                    f"Dimension mismatch error: "
                    f"Expected {self.expected_dim}, got {actual_dim} dimensions. "
                    f"Model configured: {OPENAI_EMBEDDING_MODEL}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Further validation
            self._validate_embedding(embedding)
            
            return embedding

        except Exception as e:
            error_msg = f"Error generating embedding: {str(e)}"
            logger.error(error_msg)
            raise

    def _validate_embedding(self, embedding):
        """Validate embedding structure and values."""
        if not isinstance(embedding, list):
            raise ValueError(f"Embedding must be a list, got {type(embedding)}")
            
        if len(embedding) != self.expected_dim:
            raise ValueError(
                f"Invalid embedding dimensions. Expected {self.expected_dim}, got {len(embedding)}"
            )
            
        if not all(isinstance(x, float) for x in embedding):
            raise ValueError("All embedding values must be floats")
            
        # Check for NaN or infinity values
        if any(not np.isfinite(x) for x in embedding):
            raise ValueError("Embedding contains NaN or infinity values")
            
        # Check if embedding is all zeros
        if all(x == 0 for x in embedding):
            raise ValueError("Invalid embedding: all values are zero")
            
        logger.info("Embedding validation successful")