import base64
import json
import os
from openai import OpenAI
from dotenv import load_dotenv

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


def upsert_customer_data(data, test_mode=True, logger=None):
    def log(message):
        if logger:
            logger(message)
        else:
            print(message)

    raw_phone = data.get("Phone", "").strip()
    phone_number = standardize_phone_number(raw_phone)
    if not phone_number:
        log("No valid phone number found. Skipping entry.")
        return

    email = data.get("Email", "").strip()
    email = email if is_valid_email(email) else None

    name = data.get("Name", "").strip()
    address = data.get("Address", "").strip()
    company_name = data.get("Company Name", "").strip()

    new_data = {
        "phone_number": phone_number,
        "name": name if name else None,
        "email": email,
        "address": address if address else None,
        "company_name": company_name if company_name else None
    }

    customer_table = get_table("customers", test_mode)
    existing_customers = get_existing_customers([phone_number], test_mode)

    if phone_number in existing_customers:
        customer_id = existing_customers[phone_number]["customer_id"]

        # Get the full existing record
        existing_record_resp = supabase.table(customer_table).select("*").eq("customer_id", customer_id).single().execute()
        existing_record = existing_record_resp.data if existing_record_resp.data else {}

        updated_fields = {}

        for key in ["name", "email", "address", "company_name"]:
            current_value = existing_record.get(key)
            new_value = new_data.get(key)

            if (current_value is None or current_value == "") and new_value:
                updated_fields[key] = new_value

        if updated_fields:
            log(f"Updating fields for customer {phone_number}: {updated_fields}")
            supabase.table(customer_table).update(updated_fields).eq("customer_id", customer_id).execute()
        else:
            log(f"No new fields to update for existing customer: {phone_number}")

    else:
        log(f"Inserting new customer {phone_number}")
        supabase.table(customer_table).insert(new_data).execute()



def process_all_business_cards(uploaded_files, test_mode=True, logger=None):
    def log(message):
        if logger:
            logger(message)
        else:
            print(message)

    for uploaded_file in uploaded_files:
        try:
            image_bytes = uploaded_file.read()
            data = extract_and_format_business_card(image_bytes)

            if data:
                upsert_customer_data(data, test_mode=test_mode, logger=logger)
            else:
                log(f"Failed to extract data from {uploaded_file.name}")

        except Exception as e:
            log(f"Error processing {uploaded_file.name}: {e}")
