import base64
import json
from openai import OpenAI
from dotenv import load_dotenv
import os
from src.utils import standardize_phone_number, is_valid_email

from src.data_import.db import supabase, get_table, get_existing_customers

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)


def extract_and_format_business_card(image_bytes):
    prompt = """
    Extract the following details from the business card image and format them as JSON:
    - Name
    - Email
    - Phone (If multiple, return only one)
    - Company Name
    - Address (If multiple, return only one)

    If a field is missing, return an empty string ("").
    """

    try:
        base64_image = base64.b64encode(image_bytes).decode('utf-8')
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


def upsert_customer_data_batch(parsed_data_list, test_mode=True, logger=None):
    def log(msg):
        if logger:
            logger(msg)
        else:
            print(msg)

    if not parsed_data_list:
        log("No parsed data to process.")
        return

    customer_table = get_table("customers", test_mode)

    # Prepare phone mapping and standardize numbers
    phone_map = {}
    for data in parsed_data_list:
        raw_phone = data.get("Phone", "").strip()
        phone_number = standardize_phone_number(raw_phone)
        if phone_number:
            phone_map[phone_number] = data

    phone_numbers = list(phone_map.keys())
    existing_customers = get_existing_customers(phone_numbers, test_mode)

    records_to_insert = []

    for phone_number, data in phone_map.items():
        existing = existing_customers.get(phone_number)
        name = data.get("Name", "").strip()
        email = data.get("Email", "").strip()
        company = data.get("Company Name", "").strip()
        address = data.get("Address", "").strip()

        email = email if is_valid_email(email) else None

        record = {
            "phone_number": phone_number,
            "name": name or None,
            "email": email,
            "company_name": company or None,
            "address": address or None
        }

        if existing:
            customer_id = existing["customer_id"]
            updated_fields = {}
            for field in ["name", "email", "company_name", "address"]:
                existing_val = existing.get(field)
                new_val = record.get(field)
                if (not existing_val) and new_val:
                    updated_fields[field] = new_val

            if updated_fields:
                log(f"Updating {phone_number}: {updated_fields}")
                supabase.table(customer_table).update(updated_fields).eq("customer_id", customer_id).execute()
            else:
                log(f"No update needed for existing customer: {phone_number}")
        else:
            records_to_insert.append(record)

    if records_to_insert:
        inserted_phones = [r['phone_number'] for r in records_to_insert]
        log(f"Inserting {len(records_to_insert)} new customers: {inserted_phones}")
        supabase.table(customer_table).insert(records_to_insert).execute()
    else:
        log("No new customers to insert.")


def process_all_business_cards(uploaded_files, test_mode=True, logger=None):
    def log(msg):
        if logger:
            logger(msg)
        else:
            print(msg)

    parsed_data_list = []

    for uploaded_file in uploaded_files:
        try:
            image_bytes = uploaded_file.read()
            data = extract_and_format_business_card(image_bytes)
            if data:
                parsed_data_list.append(data)
            else:
                log(f"Failed to extract data from {uploaded_file.name}")
        except Exception as e:
            log(f"Error processing {uploaded_file.name}: {e}")

    upsert_customer_data_batch(parsed_data_list, test_mode=test_mode, logger=logger)
