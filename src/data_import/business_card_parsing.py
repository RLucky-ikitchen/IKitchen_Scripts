import os
import cv2
import pytesseract
import json
import re
import argparse
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# Function to extract text from an image
def extract_text_from_image(image_path):
    try:
        image = cv2.imread(image_path)
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        text = pytesseract.image_to_string(gray)
        return text.strip()
    except Exception as e:
        print(f"Error processing {image_path}: {e}")
        return None

def format_text_with_openai(text):
    prompt = f"""
    Extract the following details from the business card text and format them in JSON:
    - Name
    - Email
    - Phone (If multiple, list all)
    - Company Name
    - Address (If multiple, list all)
    
    If a field is missing, use null as its value.

    Business Card Text:
    {text}
    """

    try:
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"}
        )

        structured_data = completion.choices[0].message.content
        return json.loads(structured_data)

    except Exception as e:
        print(f"OpenAI API Error: {e}")
        return None


# Function to process all images in a folder
def process_all_business_cards(folder_path, output_path):
    os.makedirs(output_path, exist_ok=True)
    all_data = []

    for filename in os.listdir(folder_path):
        if filename.lower().endswith((".jpg", ".jpeg", ".png")):
            image_path = os.path.join(folder_path, filename)
            extracted_text = extract_text_from_image(image_path)

            if extracted_text:  
                structured_data = format_text_with_openai(extracted_text)

                if structured_data:
                    all_data.append(structured_data)

                    json_filename = os.path.join(output_path, os.path.splitext(filename)[0] + ".json")
                    with open(json_filename, "w", encoding="utf-8") as json_file:
                        json.dump(structured_data, json_file, indent=4)

                    print(f"Processed: {filename} -> {json_filename}")
                else:
                    print(f"Skipping {filename}: OpenAI returned empty JSON.")
            else:
                print(f"Skipping {filename}: No text detected.")

    all_json_path = os.path.join(output_path, "all_business_cards.json")
    with open(all_json_path, "w", encoding="utf-8") as json_file:
        json.dump(all_data, json_file, indent=4)

    print("All business cards processed and saved!")

# Main Execution with Command-Line Arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Business Card OCR & OpenAI Formatting")
    parser.add_argument("folder_path", type=str, help="Path to folder containing images")
    parser.add_argument("output_path", type=str, help="Path to save JSON results")
    
    args = parser.parse_args()

    process_all_business_cards(args.folder_path, args.output_path)
