import os
import cv2
import pytesseract
import json
import re
from openai import OpenAI

# Set Tesseract OCR path (Google Colab has it pre-installed)
pytesseract.pytesseract.tesseract_cmd = "/usr/bin/tesseract"

# OpenAI API Key (Replace with your API key)
OPENAI_API_KEY = "OPENAI_API_KEY"
client = OpenAI(api_key=OPENAI_API_KEY)

# Path to the folder containing business cards in Google Drive
FOLDER_PATH = "/content/drive/MyDrive/Customers Business ID Card/JPEG"

# Output folder for JSON files
OUTPUT_PATH = "/content/drive/MyDrive/Customers Business ID Card/business_card_jsons"
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Function to extract text from an image
def extract_text_from_image(image_path):
    image = cv2.imread(image_path)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)  # Convert to grayscale
    text = pytesseract.image_to_string(gray)  # Perform OCR
    return text.strip()

# Function to format text into structured JSON using OpenAI
def format_text_with_openai(text):
    prompt = f"""
    Extract the following details from the business card text and format them in JSON:
    - Name
    - Email
    - Phone
    - Company Name
    - Address

    Business Card Text:
    {text}

    Output only the JSON object.
    """

    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    json_output = completion.choices[0].message.content.strip()

    # Remove Markdown JSON formatting (```json ... ```)
    json_output = re.sub(r"```json\s*|\s*```", "", json_output, flags=re.MULTILINE).strip()

    try:
        return json.loads(json_output)  # Convert to a dictionary
    except json.JSONDecodeError:
        print("❌ Error parsing JSON output:", json_output)
        return {}  # Return an empty dict if JSON parsing fails

# Function to process all images in the folder
def process_all_business_cards():
    all_data = []  # List to store JSON results

    for filename in os.listdir(FOLDER_PATH):
        if filename.lower().endswith((".jpg", ".jpeg", ".png")):
            image_path = os.path.join(FOLDER_PATH, filename)
            extracted_text = extract_text_from_image(image_path)

            if extracted_text:  # Ensure text was extracted
                structured_data = format_text_with_openai(extracted_text)

                # Save structured data only if it's not empty
                if structured_data:
                    all_data.append(structured_data)

                    # Save each business card data as a separate JSON file
                    json_filename = os.path.join(OUTPUT_PATH, os.path.splitext(filename)[0] + ".json")
                    with open(json_filename, "w", encoding="utf-8") as json_file:
                        json.dump(structured_data, json_file, indent=4)

                    print(f"✅ Processed: {filename} -> {json_filename}")
                else:
                    print(f"⚠️ Skipping {filename}: OpenAI returned empty JSON.")
            else:
                print(f"⚠️ Skipping {filename}: No text detected.")

    # Save all business card data in one JSON file
    all_json_path = os.path.join(OUTPUT_PATH, "all_business_cards.json")
    with open(all_json_path, "w", encoding="utf-8") as json_file:
        json.dump(all_data, json_file, indent=4)

    print("All business cards processed and saved!")

# Run the function
process_all_business_cards()
