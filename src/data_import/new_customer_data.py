from datetime import datetime
import pandas as pd
import argparse

from src.models import Customer
from typing import Dict

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


def process_customers(dataframe: pd.DataFrame, use_test_tables: bool = True, logger=None):
    customers_to_update = []
    customers_to_insert = []

    processed_phone_numbers = set()

    # Collect all phone numbers for batch query
    phone_numbers_to_process = dataframe['Contact Number'].dropna().apply(standardize_phone_number).dropna().unique().tolist()

    # Batch fetch existing customers
    existing_customers_data = supabase.table(get_table("customers", use_test_tables)).select("*").in_("phone_number", phone_numbers_to_process).execute()
    existing_customers_numbers = {cust['phone_number']: cust for cust in existing_customers_data.data}


    # We only process customer details if they have a phone number
    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row.get("Contact Number"))
        if pd.isna(phone_number) or not phone_number:
            continue  # Skip if no phone number

        if phone_number in processed_phone_numbers:
            if logger:
                logger(f"Found row with duplicated phone number {phone_number}, skipping customer details update.")
            continue  # Skip if phone number already processed
        processed_phone_numbers.add(phone_number)
        try:
            customer_data = {
                "phone_number": phone_number,
                "name": f"{row['First Name']} {row['Last Name']}" if not pd.isna(row['First Name']) or not pd.isna(row['Last Name']) else None,
                "email": row['Email'] if is_valid_email(row['Email']) else None,
                "address": row['Address'] if not pd.isna(row['Address']) else None,
                "company_name": row['Company Name'] if not pd.isna(row['Company Name']) else None,
                "is_VIP": 'vip' in str(row['Returning/New']).lower() or row['VIP Status'] == 'Yes',
            }
            customer = Customer(**customer_data)

        except ValueError as e:
            if logger:
                logger(f"Skipping customer due to validation error: {e}")
            continue

        existing_customer = existing_customers_numbers.get(customer.phone_number)

        if existing_customer:
            customer.customer_id = existing_customer['customer_id']
            update_data = {}

            # Only update fields if there is a value in the spreadsheet
            for field in ['name', 'email', 'address', 'company_name']:
                value = getattr(customer, field)
                if value is not None:
                    update_data[field] = value
            # Only update is_VIP if True
            if customer.is_VIP:
                update_data["is_VIP"] = True

            if update_data:
                customers_to_update.append((customer.customer_id, update_data))
        else:
            customers_to_insert.append(customer.model_dump(exclude_unset=True, exclude_none=True))
    
    
    # # Batch updates
    if logger:
        logger(f"Updating {len(customers_to_update)} customers..")
    for customer_id, updates in customers_to_update:
        supabase.table(get_table("customers", use_test_tables)).update(updates).eq("customer_id", customer_id).execute()

    # Batch inserts
    if logger:
        logger(f"Inserting {len(customers_to_insert)} customers..")
    if customers_to_insert:
        supabase.table(get_table("customers", use_test_tables)).insert(customers_to_insert).execute()


def process_customer_data(file_path, disable_test_customer_data=False, logger=None):
    """
    Process the spreadsheet and update the Supabase database.
    """
    use_test_tables = not disable_test_customer_data
    dataframe = get_spreadsheet_data(file_path)

    if logger:
        logger("Updating Customer Details")
    
    process_customers(dataframe, use_test_tables, logger)


    # if logger:
    #     logger("Importing Feedback")
    # import_feedback_data(dataframe, use_test_tables, logger)
