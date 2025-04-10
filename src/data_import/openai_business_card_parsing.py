import base64
import json
from openai import OpenAI
from dotenv import load_dotenv
import os


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


def upsert_customer_data(data, test_mode=False, logger=None):
    def log(message):
        if logger:
            logger(message)
        else:
            print(message)

    phone = data.get("Phone", "").strip()
    if not phone:
        log("No phone number found. Skipping entry.")
        return

    # Check if a customer with the same phone number exists
    existing = supabase.table("customers").select("*").eq("phone", phone).execute()

    record = {
        "name": data.get("Name", "").strip(),
        "email": data.get("Email", "").strip(),
        "phone": phone,
        "company": data.get("Company Name", "").strip(),
        "address": data.get("Address", "").strip()
    }

    if test_mode:
        log(f"\n--- Test Mode Entry ---\n{json.dumps(record, indent=4)}")
    else:
        if existing.data and len(existing.data) > 0:
            customer_id = existing.data[0]["id"]
            supabase.table("customers").update(record).eq("id", customer_id).execute()
            log(f"Updated existing customer: {phone}")
        else:
            supabase.table("customers").insert(record).execute()
            log(f"Inserted new customer: {phone}")


def process_all_business_cards(uploaded_files, test_mode=False, logger=None):
    def log(message):
        if logger:
            logger(message)
        else:
            print(message)

    for uploaded_file in uploaded_files:
        try:
            temp_path = os.path.join("temp_card.jpg")
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            data = extract_and_format_business_card(temp_path)
            if data:
                upsert_customer_data(data, test_mode=test_mode, logger=logger)
            else:
                log(f"Failed to extract data from {uploaded_file.name}")
            os.remove(temp_path)

        except Exception as e:
            log(f"Error processing {uploaded_file.name}: {e}")