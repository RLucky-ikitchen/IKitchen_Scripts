from datetime import datetime
import pandas as pd

from src.models import Customer, Feedback
from typing import List, Dict

from src.data_import.db import supabase, get_table
from src.utils import standardize_phone_number, convert_rating, is_valid_email, get_spreadsheet_data, validate_spreadsheet_columns


def get_existing_customers(phone_numbers: List[str], use_test_tables: bool) -> Dict[str, dict]:
    existing_customers_data = supabase.table(get_table("customers", use_test_tables)).select("*").in_("phone_number", phone_numbers).execute()
    return {cust['phone_number']: cust for cust in existing_customers_data.data}

def get_existing_feedback(customer_ids: List[str], test_tables: bool) -> Dict[str, dict]:
    existing_feedback_data = supabase.table(get_table("feedback", test_tables)).select("feedback_id", "customer_id").in_("customer_id", customer_ids).execute()
    return {fb['customer_id']: fb for fb in existing_feedback_data.data}


def process_customer_details(dataframe: pd.DataFrame, use_test_tables: bool = True, logger=None):
    customers_to_update = []
    customers_to_insert = []

    processed_phone_numbers = set()

    # Collect all phone numbers for batch query
    phone_numbers_to_process = dataframe['Contact Number'].dropna().apply(standardize_phone_number).dropna().unique().tolist()
    existing_customers_numbers = get_existing_customers(phone_numbers_to_process, use_test_tables)

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
    
    
    # Batch updates
    if logger:
        logger(f"Updating {len(customers_to_update)} customers..")
    for customer_id, updates in customers_to_update:
        supabase.table(get_table("customers", use_test_tables)).update(updates).eq("customer_id", customer_id).execute()

    # Batch inserts
    if logger:
        logger(f"Inserting {len(customers_to_insert)} customers..")
    if customers_to_insert:
        supabase.table(get_table("customers", use_test_tables)).insert(customers_to_insert).execute()


def process_feedback(dataframe: pd.DataFrame, use_test_tables: bool = True, logger=None):
    feedbacks_to_insert = []
    feedbacks_to_update = []

    # Collect all existing feedback
    phone_numbers_to_process = dataframe['Contact Number'].dropna().apply(standardize_phone_number).dropna().unique().tolist()
    existing_customers_numbers = get_existing_customers(phone_numbers_to_process, use_test_tables)
    customer_ids = [cust['customer_id'] for cust in existing_customers_numbers.values()]
    existing_feedback = get_existing_feedback(customer_ids, use_test_tables)

    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row.get("Contact Number"))
        if pd.isna(phone_number) or not phone_number:
            continue  # Skip if no phone number

        customer_id = existing_customers_numbers[phone_number]['customer_id']

        feedback_date = pd.to_datetime(row.get('Date')).isoformat() if not pd.isna(row['Date']) else None

        feedback_data = Feedback(
            customer_id=customer_id,
            food_review=convert_rating(row.get('Food Review')),
            service=convert_rating(row.get('Service')),
            cleanliness=convert_rating(row.get('Cleanliness')),
            atmosphere=convert_rating(row.get('Atmosphere')),
            value=convert_rating(row.get('Value')),
            where_did_they_hear_about_us=row.get('Where did they hear from us?') if not pd.isna(row['Where did they hear from us?']) else None,
            overall_experience=convert_rating(row.get('Overall Experience')),
            feedback_date=feedback_date if feedback_date else datetime.now().isoformat(),
        )
        
        # Only process feedback if not empty
        if any([feedback_data.food_review, feedback_data.service, feedback_data.cleanliness,
                feedback_data.atmosphere, feedback_data.value, feedback_data.overall_experience]):
            existing_fb = existing_feedback.get(feedback_data.customer_id)
            if existing_fb:
                feedbacks_to_update.append({"feedback_id": existing_fb['feedback_id'], **feedback_data.model_dump(exclude_none=True)})
                if logger:
                    logger(f"New feedback for customer {customer_id}")
            else:
                feedbacks_to_insert.append(feedback_data.model_dump(exclude_none=True))

    # Batch inserts
    if feedbacks_to_insert:
        if logger:
            logger(f"Inserting {len(feedbacks_to_insert)} new feedback entries")
        supabase.table(get_table("feedback", use_test_tables)).insert(feedbacks_to_insert).execute()
        

    # Batch updates
    if feedbacks_to_update:
        if logger:
            logger(f"Updatinf {len(feedbacks_to_update)} feedback entries")
        for feedback in feedbacks_to_update:
            feedback_id = feedback.pop("feedback_id")
            supabase.table(get_table("feedback", use_test_tables)).update(feedback).eq("feedback_id", feedback_id).execute()
        

def process_customer_data(file_path, disable_test_customer_data=False, logger=None):
    """
    Process the spreadsheet and update the Supabase database.
    """
    use_test_tables = not disable_test_customer_data
    dataframe = get_spreadsheet_data(file_path)
    
    # We must first create or update all customers
    validate_spreadsheet_columns(dataframe, "customer_details")
    process_customer_details(dataframe, use_test_tables, logger)

    # Then we process the feedback
    validate_spreadsheet_columns(dataframe, "feedback")
    process_feedback(dataframe, use_test_tables, logger)
