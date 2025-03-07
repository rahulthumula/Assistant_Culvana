from openai import OpenAI
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()
api_key1 = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key1)

# Replace with your actual job ID from when you created the fine-tuning job
job_id = "ftjob-TDq774Mkdfy2CuIp557SNLwC" # Replace with your job ID

# Retrieve the job details
job = client.fine_tuning.jobs.retrieve(job_id)

# Check if the job has completed successfully
print(f"Job status: {job.status}")
if job.status == "succeeded":
    fine_tuned_model_id = job.fine_tuned_model
    print(f"Your fine-tuned model ID: {fine_tuned_model_id}")
else:
    print(f"Job status: {job.status}")
    print("The fine-tuning job hasn't completed successfully yet.")