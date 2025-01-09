from supabase import create_client, Client
from utils import standardize_phone_number
from utils import convert_rating
from dotenv import load_dotenv
from utils import is_valid_email
import pandas as pd
import argparse
import os

# Load environment variables
load_dotenv(".env")

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def import_feedback_data(dataframe):
    """
    Import feedback data into Supabase.
    """
    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row['Contact Number'])
        if not phone_number:
            continue

        # Fetch the customer ID
        customer = supabase.table("customers").select("customer_id").eq("phone_number", phone_number).execute()
        customer_id = None

        if customer.data:
            customer_id = customer.data[0]['customer_id']
        else:
            # Prepare the new customer data
            email = row['Email']
            if not is_valid_email(email):  # Exclude invalid email values
                email = None

            new_customer = {
                "phone_number": phone_number,
                "name": f"{row['First Name']} {row['Last Name']}".strip(),
                "address": row['Address'],
                "email": email,
                "company_name": row['Company Name']
            }

            # Remove invalid fields (e.g., None or NaN values)
            new_customer = {k: v for k, v in new_customer.items() if pd.notna(v) and v != ""}

            # Insert the new customer
            created_customer = supabase.table("customers").insert(new_customer).execute()
            customer_id = created_customer.data[0]['customer_id'] if created_customer.data else None

        if customer_id:
            # Map and convert ratings using convert_rating function
            feedback_data = {
                "customer_id": customer_id,
                "food_review": convert_rating(row.get('Food Review', None)),
                "service": convert_rating(row.get('Service', None)),
                "cleanliness": convert_rating(row.get('Cleanliness', None)),
                "atmosphere": convert_rating(row.get('Atmosphere', None)),
                "value": convert_rating(row.get('Value', None)),
                "where_did_they_hear_about_us": row.get('Where did they hear from us?', None),
                "overall_experience": convert_rating(row.get('Overall Experience', None)),
                "feedback_date": row.get('Feedback Date', None),
            }

            # Remove invalid fields from feedback data
            feedback_data = {k: v for k, v in feedback_data.items() if pd.notna(v)}

            # Skip empty feedback (all numeric fields are 0 and feedback_text is empty or None)
            feedback_fields = [
                feedback_data.get("food_review", 0),
                feedback_data.get("service", 0),
                feedback_data.get("cleanliness", 0),
                feedback_data.get("atmosphere", 0),
                feedback_data.get("value", 0),
                feedback_data.get("overall_experience", 0),
            ]
            feedback_text = feedback_data.get("feedback_text", "").strip()
            if all(field == 0 for field in feedback_fields) and not feedback_text:
                print(f"Skipping empty feedback for customer {customer_id}")
                continue

            # Insert feedback
            supabase.table("feedback").insert(feedback_data).execute()



def update_customer_details(dataframe):
    """
    Update customer details in Supabase based on phone number.
    """
    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row['Contact Number'])
        if not phone_number:
            continue

        customer = supabase.table("customers").select("*").eq("phone_number", phone_number).execute()
        if customer.data:
            customer_id = customer.data[0]['customer_id']
            updates = {}

            if not pd.isna(row['First Name']) or not pd.isna(row['Last Name']):
                updates['name'] = f"{row['First Name']} {row['Last Name']}"

            if not pd.isna(row['Address']):
                updates['address'] = row['Address']

            if not pd.isna(row['Email']):
                email = row['Email']
                if not is_valid_email(email):
                    print(f"Skipping email update for customer {customer_id} due to invalid email: {email}")
                else:
                    # Check if the email exists for another customer
                    email_check = supabase.table("customers").select("customer_id").eq("email", email).execute()
                    if email_check.data and email_check.data[0]['customer_id'] != customer_id:
                        print(f"Skipping email update for customer {customer_id} due to duplicate email: {email}")
                    else:
                        updates['email'] = email

            if not pd.isna(row['Company Name']):
                updates['company_name'] = row['Company Name']

            if updates:
                supabase.table("customers").update(updates).eq("customer_id", customer_id).execute()

def update_customer_status(dataframe):
    """
    Update customer status in Supabase.
    """
    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row['Contact Number'])
        status = row['VIP/Returning/New']

        if not phone_number or status == 'New':
            continue

        customer = supabase.table("customers").select("*").eq("phone_number", phone_number).execute()

        updates = {}
        if status == 'VIP':
            updates['is_VIP'] = True
        elif status == 'Returning':
            updates['is_returning_customer'] = True

        if customer.data:
            customer_id = customer.data[0]['customer_id']
            supabase.table("customers").update(updates).eq("customer_id", customer_id).execute()
        else:
            new_customer = {
                "phone_number": phone_number,
                "name": f"{row['First Name']} {row['Last Name']}",
                "address": row['Address'],
                "email": row['Email'] if is_valid_email(row['Email']) else None,
                "company_name": row['Company Name'],
                **updates
            }
            supabase.table("customers").insert(new_customer).execute()

def process_customer_data(file_path, sheet_name):
    """
    Process the spreadsheet and update the Supabase database.
    """
    dataframe = pd.read_excel(file_path, sheet_name=sheet_name)
    update_customer_details(dataframe)
    import_feedback_data(dataframe)
    update_customer_status(dataframe)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process customer data from an Excel file.")
    parser.add_argument("file_path", type=str, help="Path to the Excel file containing customer data.")
    parser.add_argument("sheet_name", type=str, help="Name of the sheet in the Excel file to process.")
    args = parser.parse_args()

    # Call the main processing function with the provided file path and sheet name
    process_customer_data(args.file_path, args.sheet_name)