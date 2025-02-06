from datetime import datetime
import pandas as pd
import argparse

from src.data_import.db import supabase, get_table
from src.utils import standardize_phone_number, convert_rating, is_valid_email, get_spreadsheet_data


def import_feedback_data(dataframe, test_tables, logger=None):
    """
    Import feedback data into Supabase.
    """
    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row['Contact Number'])
        if not phone_number:
            continue

        # Fetch the customer ID
        customer = supabase.table(get_table("customers", test_tables)).select("customer_id").eq("phone_number", phone_number).execute()
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
            created_customer = supabase.table(get_table("customers", test_tables)).insert(new_customer).execute()
            customer_id = created_customer.data[0]['customer_id'] if created_customer.data else None

        if customer_id:
            # Map and convert ratings using convert_rating function

            feedback_date = row.get('Feedback Date', None)
            if pd.isna(feedback_date):
                feedback_date = datetime.now().isoformat()  # Use current date/time as fallback
            else:
                feedback_date = pd.to_datetime(feedback_date).isoformat()  # Ensure correct format

            feedback_data = {
                "customer_id": customer_id,
                "food_review": convert_rating(row.get('Food Review', None)),
                "service": convert_rating(row.get('Service', None)),
                "cleanliness": convert_rating(row.get('Cleanliness', None)),
                "atmosphere": convert_rating(row.get('Atmosphere', None)),
                "value": convert_rating(row.get('Value', None)),
                "where_did_they_hear_about_us": row.get('Where did they hear from us?', None),
                "overall_experience": convert_rating(row.get('Overall Experience', None)),
                "feedback_date": feedback_date,  # Always has a valid value
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
                if logger:
                    logger(f"Skipping empty feedback for customer {customer_id}")
                continue

            # Check if feedback for the same date already exists for the customer
            existing_feedback = supabase.table(get_table("feedback", test_tables)).select("feedback_id").eq("customer_id", customer_id).execute()
            if existing_feedback.data:
                # Update existing feedback
                feedback_id = existing_feedback.data[0]["feedback_id"]
                supabase.table(get_table("feedback", test_tables)).update(feedback_data).eq("feedback_id", feedback_id).execute()
                if logger:
                    logger(f"Updated feedback for customer {customer_id}")
            else:
                # Insert new feedback
                supabase.table(get_table("feedback", test_tables)).insert(feedback_data).execute()
                if logger:
                    logger(f"Inserted new feedback for customer {customer_id}")

            return True


def update_customer_details(dataframe, test_tables, logger=None):
    """
    Update customer details in Supabase based on phone number.
    """
    for i, row in dataframe.iterrows():
        if logger and i % 20 == 0:
            logger(f"Processed {i} customers")
        phone_number = standardize_phone_number(row['Contact Number'])
        if not phone_number:
            continue

        customer = supabase.table(get_table("customers", test_tables)).select("*").eq("phone_number", phone_number).execute()
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
                    if logger:
                        logger(f"Skipping email update for customer {customer_id} due to invalid email: {email}")
                else:
                    # Check if the email exists for another customer
                    email_check = supabase.table(get_table("customers", test_tables)).select("customer_id").eq("email", email).execute()
                    if email_check.data and email_check.data[0]['customer_id'] != customer_id:
                        if logger:
                            logger(f"Skipping email update for customer {customer_id} due to duplicate email: {email}")
                    else:
                        updates['email'] = email

            if not pd.isna(row['Company Name']):
                updates['company_name'] = row['Company Name']

            if updates:
                supabase.table(get_table("customers", test_tables)).update(updates).eq("customer_id", customer_id).execute()


def update_customer_vip_status(dataframe, test_tables, logger=None):
    """
    Update customer status in Supabase.
    """
    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row['Contact Number'])
        if not phone_number:
            continue

        if 'VIP' in str(row['Returning/New']) or row['VIP Status'] == 'Yes':

            customer = supabase.table(get_table("customers", test_tables)).select("*").eq("phone_number", phone_number).execute()

            if customer.data:
                customer_id = customer.data[0]['customer_id']
                supabase.table(get_table("customers", test_tables)).update({'is_VIP': True}).eq("customer_id", customer_id).execute()
            else:
                new_customer = {
                    "phone_number": phone_number,
                    "name": f"{row['First Name']} {row['Last Name']}",
                    'is_VIP': True
                }
                if not pd.isna(row["Address"]):
                    new_customer["address"] = row['Address']
                if not pd.isna(row["Company Name"]):
                    new_customer["company_name"] = row['Company Name']

                supabase.table(get_table("customers", test_tables)).insert(new_customer).execute()

def process_customer_data(file_path, test_tables=False, logger=None):
    """
    Process the spreadsheet and update the Supabase database.
    """
    dataframe = get_spreadsheet_data(file_path)

    if logger:
        logger("Updating Customer Details")
    # update_customer_details(dataframe, test_tables, logger)
    # update_customer_vip_status(dataframe, test_tables, logger)

    if logger:
        logger("Importing Feedback")
    import_feedback_data(dataframe, test_tables, logger)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process customer data from an Excel file.")
    parser.add_argument("file_path", type=str, help="Path to the Excel file containing customer data.")
    parser.add_argument("sheet_name", type=str, help="Name of the sheet in the Excel file to process.")
    args = parser.parse_args()

    # Call the main processing function with the provided file path and sheet name
    process_customer_data(args.file_path, args.sheet_name)
