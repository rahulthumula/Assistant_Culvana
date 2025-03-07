import asyncio
from embeddings import EmbeddingGenerator

async def test_embeddings():
    """Test function to verify embedding generation"""
    try:
        print("\nStarting embedding test...")
        print(f"Initializing EmbeddingGenerator...")
        generator = EmbeddingGenerator()
        
        # Test with a simple text
        test_text = "This is a test sentence."
        print(f"\nGenerating embedding for: '{test_text}'")
        embedding = await generator.generate_embedding(test_text)
        
        print("\nEmbedding test results:")
        print(f"Dimensions: {len(embedding)}")
        print(f"First 5 values: {embedding[:5]}")
        print(f"Value range: {min(embedding)} to {max(embedding)}")
        print("\nTest completed successfully!")
        
    except Exception as e:
        print(f"Test failed with error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_embeddings())