from openai import OpenAI
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
api_key1 = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key1)

# First, upload the file
with open("restaurant_inventory_qa.jsonl", "rb") as file:
    uploaded_file = client.files.create(
        file=file,
        purpose="fine-tune"
    )

# Then create the fine-tuning job with the file ID
job = client.fine_tuning.jobs.create(
    training_file=uploaded_file.id,  # Use the file ID, not the filename
    model="gpt-4o-2024-08-06",  # Use a supported model
    hyperparameters={
        "n_epochs": 3
    }
)

print(f"Fine-tuning job created with ID: {job.id}")
print(f"Status: {job.status}")