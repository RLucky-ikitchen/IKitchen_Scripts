import os
import re
import json
from datetime import datetime
from promptlayer import PromptLayer
import requests
from supabase import create_client, Client
from src.data_import.db import get_table, get_existing_customers
from src.utils import standardize_phone_number
import traceback
from mutagen.mp3 import MP3

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# PromptLayer and ElevenLabs setup
promptlayer_client = PromptLayer(api_key=os.environ["PROMPTLAYER_API_KEY"])
ELEVENLABS_API_KEY = os.environ["ELEVENLABS_API_KEY"]
ELEVENLABS_MODEL_ID = os.environ.get("ELEVENLABS_MODEL_ID", "scribe_v1")
ELEVENLABS_STT_URL = "https://api.elevenlabs.io/v1/speech-to-text"

# Supabase setup
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def extract_date_and_phone(filename):
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

def get_audio_duration_seconds(file_path):
    try:
        audio = MP3(file_path)
        return float(getattr(getattr(audio, "info", None), "length", None))
    except Exception:
        return None

def transcribe_audio(file_path):
    with open(file_path, 'rb') as f:
        files = {'file': (os.path.basename(file_path), f, 'audio/mpeg')}
        data = {'model_id': ELEVENLABS_MODEL_ID}
        response = requests.post(
            ELEVENLABS_STT_URL,
            headers={"xi-api-key": ELEVENLABS_API_KEY},
            files=files,
            data=data
        )
        response.raise_for_status()
        return response.json().get("text", "")

def extract_facts(transcript):
    input_variables = {"transcript": transcript}
    response = promptlayer_client.run(prompt_name="IVR_fact_extraction", input_variables=input_variables)
    return json.loads(response["raw_response"].choices[0].message.content)

def none_if_empty(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value

def update_customer_info(customer_id, extracted, current_data, test_mode):
    updates = {}
    for field in ["name", "company_name", "address", "email"]:
        if not current_data.get(field) and extracted.get(field):
            updates[field] = extracted[field]
    if updates:
        customer_table = get_table("customers", test_mode)
        supabase.table(customer_table).update(updates).eq("customer_id", customer_id).execute()

def insert_transcripts_batch(transcripts_payload, test_mode):
    transcript_table = get_table("ivr_transcripts", test_mode)
    if transcripts_payload:
        try:
            supabase.table(transcript_table).insert(transcripts_payload).execute()
            print("✅ Batch insert of transcripts successful.")
        except Exception as e:
            print(f"❌ Batch insert failed: {e}")
            raise

def insert_memories_batch(memory_payloads, test_mode):
    memory_table = get_table("memory", test_mode)
    if memory_payloads:
        try:
            supabase.table(memory_table).insert(memory_payloads).execute()
        except Exception as e:
            print(f"❌ Error inserting memory batch: {e}")

def process_audio_files(uploaded_files, test_mode=True, logger=print):
    all_phones = []
    file_info = []

    for uploaded_file in uploaded_files:
        file_name = uploaded_file.name
        date, raw_phone = extract_date_and_phone(file_name)
        phone = standardize_phone_number(raw_phone)

        if not (date and phone):
            logger(f"Skipping {file_name}: couldn't extract valid date or phone")
            continue

        all_phones.append(phone)
        file_info.append((uploaded_file, file_name, date, phone))

    customer_map = get_existing_customers(all_phones, test_mode)
    existing_transcripts = supabase.table("ivr_transcripts").select("recording").execute().data
    processed_recordings = set(row["recording"] for row in existing_transcripts if row["recording"])

    transcripts_payload = []
    memory_payloads = []

    for uploaded_file, file_name, date, phone in file_info:
        if file_name in processed_recordings:
            logger(f"⏩ Skipping already processed file: {file_name}")
            continue

        logger(f"Processing {file_name} (Date: {date}, Phone: {phone})")

        try:
            with open(f"temp_{file_name}", "wb") as temp_file:
                temp_file.write(uploaded_file.read())
                temp_path = temp_file.name

            duration_seconds = get_audio_duration_seconds(temp_path)
            if duration_seconds is not None and duration_seconds < 10:
                logger(f"⏩ Skipping short audio ({duration_seconds:.1f}s): {file_name}")
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
                continue

            transcript = transcribe_audio(temp_path)
            extracted = extract_facts(transcript)

            extracted_phone = phone  # always use filename phone
            customer = customer_map.get(extracted_phone)

            if customer:
                customer_id = customer["customer_id"]
            else:
                customer_table = get_table("customers", test_mode)
                try:
                    insert_res = supabase.table(customer_table).insert({"phone_number": extracted_phone}).execute()
                    customer_id = insert_res.data[0]["customer_id"]
                except Exception:
                    # Likely a duplicate; fetch the existing customer_id
                    existing = supabase.table(customer_table).select("customer_id").eq("phone_number", extracted_phone).limit(1).execute()
                    if existing.data:
                        customer_id = existing.data[0]["customer_id"]
                    else:
                        raise
                customer = {"phone_number": extracted_phone, "customer_id": customer_id}
                customer_map[extracted_phone] = customer

            update_customer_info(customer_id, extracted, customer, test_mode)

            transcripts_payload.append({
                "customer_id": customer_id,
                "content": transcript,
                "date_recording": date,
                "sentiment": none_if_empty(extracted.get("sentiment")),
                "recording": file_name,
                "category": none_if_empty(extracted.get("category"))
            })

            memory_content = []
            for key, value in extracted.items():
                if key in ["name", "company_name", "address", "email", "phone_number", "sentiment"]:
                    continue
                if value:
                    memory_content.append(f"{key}: {value}")

            if memory_content:
                memory_payloads.append({
                    "customer_id": customer_id,
                    "content": ", ".join(memory_content),
                    "source": "transcript"
                })

            logger(f"✅ Processed {file_name}")

        except Exception as e:
            error_msg = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            logger(f"❌ Error processing {file_name}: {error_msg}")

    insert_transcripts_batch(transcripts_payload, test_mode)
    insert_memories_batch(memory_payloads, test_mode)