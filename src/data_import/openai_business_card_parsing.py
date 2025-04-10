import base64
import json
from openai import OpenAI
from dotenv import load_dotenv
import os


from src.data_import.db import supabase, get_table, get_existing_customers

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)


def extract_and_format_business_card_from_bytes(image_bytes):
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


def process_all_business_cards(data: dict, use_test_tables: bool = False):
    customers_table = get_table("customers", use_test_tables)

    phone = data.get("Phone")
    if not phone:
        print("No phone number found, skipping entry.")
        return False

    existing_customers = get_existing_customers([phone], use_test_tables)
    cleaned_data = {
        "name": data.get("Name", ""),
        "email": data.get("Email", ""),
        "phone_number": phone,
        "company": data.get("Company Name", ""),
        "address": data.get("Address", "")
    }

    if phone in existing_customers:
        existing = existing_customers[phone]
        update_payload = {
            key: value
            for key, value in cleaned_data.items()
            if value and (not existing.get(key) or existing.get(key) == "")
        }
        if update_payload:
            supabase.table(customers_table).update(update_payload).eq("phone_number", phone).execute()
    else:
        supabase.table(customers_table).insert(cleaned_data).execute()

    return True
