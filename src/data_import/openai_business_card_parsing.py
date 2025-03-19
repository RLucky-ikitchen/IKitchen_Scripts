import os
import json
import argparse
import base64
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# Function to extract and format business card data using OpenAI Vision
def extract_and_format_business_card(image_path):
    prompt = """
    Extract the following details from the business card image and format them as JSON:
    - Name
    - Email
    - Phone (If multiple, list all)
    - Company Name
    - Address (If multiple, list all)

    If a field is missing, use null as its value.
    """

    try:
        # Read image and encode as base64
        with open(image_path, "rb") as image_file:
            image_data = image_file.read()
            base64_image = base64.b64encode(image_data).decode('utf-8')
            data_url = f"data:image/jpeg;base64,{base64_image}"
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an AI that extracts structured data from business cards."},
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}}
                ]}
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )

        structured_data = response.choices[0].message.content
        return json.loads(structured_data)

    except Exception as e:
        print(f"OpenAI API Error: {e}")
        return None

# Function to process all business cards in a folder
def process_all_business_cards(folder_path, output_path):
    os.makedirs(output_path, exist_ok=True)
    all_data = []

    for filename in os.listdir(folder_path):
        if filename.lower().endswith((".jpg", ".jpeg", ".png")):
            image_path = os.path.join(folder_path, filename)
            structured_data = extract_and_format_business_card(image_path)

            if structured_data:
                all_data.append(structured_data)

                json_filename = os.path.join(output_path, os.path.splitext(filename)[0] + ".json")
                with open(json_filename, "w", encoding="utf-8") as json_file:
                    json.dump(structured_data, json_file, indent=4)

                print(f"Processed: {filename} -> {json_filename}")
            else:
                print(f"Failed to process {filename}")

    all_json_path = os.path.join(output_path, "all_business_cards.json")
    with open(all_json_path, "w", encoding="utf-8") as json_file:
        json.dump(all_data, json_file, indent=4)

    print(f"All business cards processed and saved to {all_json_path}!")

# Main Execution with Command-Line Arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Business Card Extraction using OpenAI Vision")
    parser.add_argument("folder_path", type=str, help="Path to folder containing images")
    parser.add_argument("output_path", type=str, help="Path to save JSON results")
    
    args = parser.parse_args()

    process_all_business_cards(args.folder_path, args.output_path)