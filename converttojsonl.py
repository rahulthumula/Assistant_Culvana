import re
import json

def extract_qa_pairs(text):
    """
    Extract question and answer pairs from text using regex pattern matching.
    Looking for patterns like "Q1: question text" followed by "A1: answer text"
    """
    # Pattern to match Q and A with matching numbers
    pattern = r'Q(\d+): (.*?)\nA\1: (.*?)(?=\n\nQ\d+:|$)'
    
    # Find all matches in the text (using DOTALL to match across line breaks)
    matches = re.findall(pattern, text, re.DOTALL)

    # Return list of tuples (question_number, question, answer)
    return [(num, question.strip(), answer.strip()) for num, question, answer in matches]

def convert_to_openai_format(qa_pairs):
    """
    Convert extracted Q&A pairs to OpenAI fine-tuning format
    """
    formatted_data = []
    
    for _, question, answer in qa_pairs:
        data_entry = {
            "messages": [
                {
                    "role": "system", 
                    "content": "You are a helpful restaurant management assistant."
                },
                {
                    "role": "user", 
                    "content": question
                },
                {
                    "role": "assistant", 
                    "content": answer
                }
            ]
        }
        # Convert to JSON string
        formatted_data.append(json.dumps(data_entry))
    
    return formatted_data

def save_to_jsonl(formatted_data, output_file="restaurant_inventory_qa.jsonl"):
    """
    Save the formatted data to a JSONL file (one JSON object per line)
    """
    with open(output_file, 'w') as f:
        for entry in formatted_data:
            f.write(f"{entry}\n")
    return output_file

def main():
    with open('Inventory Management Questions.txt', 'r') as file:
        content = file.read()
    
    # Extract Q&A pairs
    qa_pairs = extract_qa_pairs(content)
    print(f"Extracted {len(qa_pairs)} Q&A pairs")
    
    # Convert to OpenAI format
    openai_format = convert_to_openai_format(qa_pairs)
    
    # Save to file
    output_file = save_to_jsonl(openai_format)
    print(f"Saved {len(openai_format)} training examples to {output_file}")
    
    # Print a few examples
    if openai_format:
        print("\nFirst example:")
        print(openai_format[0])
        
        if len(openai_format) > 1:
            print("\nSecond example:")
            print(openai_format[1])
    
    return openai_format

if __name__ == "__main__":
    main()