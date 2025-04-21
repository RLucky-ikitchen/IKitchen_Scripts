import os
import re
import json
import argparse
from glob import glob
from datetime import datetime
from dotenv import load_dotenv
from promptlayer import PromptLayer
import requests

# Load environment variables from .env
load_dotenv()

# Set up PromptLayer client
promptlayer_client = PromptLayer(api_key=os.environ["PROMPTLAYER_API_KEY"])

# ElevenLabs API setup
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")
ELEVENLABS_MODEL_ID = os.environ.get("ELEVENLABS_MODEL_ID", "scribe_v1")
ELEVENLABS_STT_URL = "https://api.elevenlabs.io/v1/speech-to-text"


def extract_date_and_phone(filename):
    # Extract date (first 8-digit number) and phone (first 11-digit number) from filename
    date_match = re.search(r"(\d{8})", filename)
    phone_match = re.search(r"(\d{11})", filename)
    date = date_match.group(1) if date_match else None
    phone = phone_match.group(1) if phone_match else None

    if date:
        try:
            date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass

    return date, phone


def transcribe_audio(file_path):
    print(f"Sending file to ElevenLabs STT: {file_path}")
    try:
        with open(file_path, 'rb') as f:
            files = {
                'file': (os.path.basename(file_path), f, 'audio/mpeg')
            }
            data = {
                'model_id': ELEVENLABS_MODEL_ID
            }
            response = requests.post(
                ELEVENLABS_STT_URL,
                headers={"xi-api-key": ELEVENLABS_API_KEY},
                files=files,
                data=data
            )
            response.raise_for_status()
            return response.json().get("text", "")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error: {http_err}")
        print(f"Response: {response.text}")
        raise


def extract_facts(transcript):
    input_variables = {
        "transcript": transcript
    }
    response = promptlayer_client.run(
        prompt_name="IVR_fact_extraction",
        input_variables=input_variables
    )
    return json.loads(response["raw_response"].choices[0].message.content)


def process_directory(directory):
    mp3_files = glob(os.path.join(directory, "*.mp3"))
    if not mp3_files:
        print(f"No .mp3 files found in {directory}")
        return

    for file_path in mp3_files:
        filename = os.path.basename(file_path)
        date, phone = extract_date_and_phone(filename)
        if not (date and phone):
            print(f"Skipping {filename}: couldn't extract date or phone")
            continue

        print(f"Processing {filename} (Date: {date}, Phone: {phone})")

        try:
            transcript = transcribe_audio(file_path)
            extracted_data = extract_facts(transcript)
            output = {
                "date": date,
                "phone": phone,
                "transcript": transcript,
                "extracted_content": extracted_data
            }
            print(json.dumps(output, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"Error processing {filename}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Process Pendulum IVR audio recordings.")
    parser.add_argument("directory", help="Path to directory containing .mp3 files")
    args = parser.parse_args()
    process_directory(args.directory)


if __name__ == "__main__":
    main()
