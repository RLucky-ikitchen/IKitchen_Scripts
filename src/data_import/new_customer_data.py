from datetime import datetime
import pandas as pd

from src.models import Customer, Feedback


from src.data_import.db import supabase, get_table, get_existing_customers, get_existing_feedback, get_existing_orders, refresh_views_analytics
from src.utils import standardize_phone_number, convert_rating, is_valid_email, get_spreadsheet_data, validate_spreadsheet_columns


def get_phone_numbers_to_process(dataframe):
    return dataframe['Contact Number'].dropna().apply(standardize_phone_number).dropna().unique().tolist()


def process_customer_details(dataframe: pd.DataFrame, use_test_tables: bool = True, logger=None):
    customers_to_update = {}
    customers_to_insert = {}

    # Collect all phone numbers for batch query
    phone_numbers_to_process = get_phone_numbers_to_process(dataframe)
    existing_customers_numbers = get_existing_customers(phone_numbers_to_process, use_test_tables)

    # We only process customer details if they have a phone number
    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row.get("Contact Number"))
        if pd.isna(phone_number) or not phone_number:
            continue  # Skip if no phone number

        try:
            customer_data = {
                "phone_number": phone_number,
                "name": f"{row['First Name']} {row['Last Name']}" if not pd.isna(row['First Name']) or not pd.isna(row['Last Name']) else None,
                "email": row['Email'] if is_valid_email(row['Email']) else None,
                "address": row['Address'] if not pd.isna(row['Address']) else None,
                "company_name": row['Company Name'] if not pd.isna(row['Company Name']) else None,
                "is_VIP": 'vip' in str(row['Returning']).lower() or row['VIP Status'] == 'Yes',
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

            # Only update fields if there is a value in the spreadsheet and if the existing customer doesn't already have that value
            for field in ['name', 'email', 'address', 'company_name']:
                current_value = existing_customer[field]
                new_value = getattr(customer, field)
        
                if not current_value and new_value is not None:
                    update_data[field] = new_value
            # Only update is_VIP if True
            if customer.is_VIP and not existing_customer["is_VIP"]:
                update_data["is_VIP"] = True

            if update_data:
                # Using customer_id as the key
                if logger:
                    logger(f"Updating customer {phone_number} with {update_data}")
                if customer.customer_id in customers_to_update:
                    customers_to_update[customer.customer_id].update(update_data)
                else:
                    customers_to_update[customer.customer_id] = update_data
        else:
            # Check if the customer is already in customers_to_insert 
            # - this happens if the same customer appears multiple times in the spreadsheet.
            if phone_number in customers_to_insert:
                existing_insert = customers_to_insert[phone_number]
                # Update missing fields
                for field in ['name', 'email', 'address', 'company_name']:
                    value = getattr(customer, field)
                    if value and (field not in existing_insert or not existing_insert[field]):
                        existing_insert[field] = value

                if customer.is_VIP:
                    existing_insert["is_VIP"] = True
            else:
                customers_to_insert[phone_number] = customer.model_dump(exclude_unset=True, exclude_none=True)
    
    # Update existing customers
    if logger:
        logger(f"Updating {len(customers_to_update)} customers..")
    for customer_id, updates in customers_to_update.items():
        supabase.table(get_table("customers", use_test_tables)).update(updates).eq("customer_id", customer_id).execute()

    # Batch inserts list of new customers
    if logger:
        logger(f"Inserting {len(customers_to_insert)} customers..")
    if customers_to_insert:
        supabase.table(get_table("customers", use_test_tables)).insert(list(customers_to_insert.values())).execute()


def process_order_mappings(dataframe: pd.DataFrame, use_test_tables: bool = True, logger=None):
    phone_numbers_to_process = get_phone_numbers_to_process(dataframe)

    # Generate formatted receipt_ids: "Receipt No._dd_mm_yyyy"
    formatted_receipt_ids = []
    for _, row in dataframe.iterrows():
        receipt_number = row.get("Receipt No.")
        date_str = row.get("Date")
        order_date = pd.to_datetime(date_str, format="%b/%d/%Y", errors="coerce")
        if pd.notna(receipt_number) and pd.notna(order_date):
            formatted_date = order_date.strftime("%d_%m_%Y")
            formatted_receipt_id = f"{receipt_number}_{formatted_date}"
            formatted_receipt_ids.append(formatted_receipt_id)

    # Remove duplicates before fetching
    formatted_receipt_ids = list(set(formatted_receipt_ids))

    existing_customers = get_existing_customers(phone_numbers_to_process, use_test_tables)
    existing_orders = get_existing_orders(formatted_receipt_ids, use_test_tables)

    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row.get("Contact Number"))
        receipt_number = row.get("Receipt No.")
        order_date = pd.to_datetime(row.get("Date"))

        if pd.notna(phone_number) and pd.notna(receipt_number) and pd.notna(order_date):
            formatted_date = order_date.strftime("%d_%m_%Y")
            formatted_receipt_id = f"{receipt_number}_{formatted_date}"

            existing_customer = existing_customers.get(phone_number)
            order = existing_orders.get(formatted_receipt_id)

            if existing_customer and order:
                if not order.get('customer_id'):
                    customer_id = existing_customer['customer_id']
                    supabase.table(get_table("orders", use_test_tables)) \
                        .update({"customer_id": customer_id}) \
                        .eq("receipt_id", formatted_receipt_id) \
                        .execute()
                    if logger:
                        logger(f"Mapping order {formatted_receipt_id} to customer {phone_number}")



def normalize_feedback_source(source: str) -> str | None:
    if pd.isna(source):
        return None

    source = source.strip()

    if source == "Passing by":
        return "Passing by"
    elif source == "Friends and Family" or source == "Family and Friends":
        return "Friends and Family"
    elif source in {"Facebook", "Instagram", "Ads"}:
        return "Social Media"
    elif source == "Social Media":
        return "Social Media"
    else:
        return None


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
            where_did_they_hear_about_us=normalize_feedback_source(row.get('Where did they hear from us?')),
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
            logger(f"Updating {len(feedbacks_to_update)} feedback entries")
        for feedback in feedbacks_to_update:
            feedback_id = feedback.pop("feedback_id")
            supabase.table(get_table("feedback", use_test_tables)).update(feedback).eq("feedback_id", feedback_id).execute()
        
def process_memory_entries(dataframe: pd.DataFrame, use_test_tables: bool = True, logger=None):
    memory_entries = []

    # Collect all phone numbers to map them to existing customers
    phone_numbers_to_process = get_phone_numbers_to_process(dataframe)
    existing_customers_numbers = get_existing_customers(phone_numbers_to_process, use_test_tables)

    for _, row in dataframe.iterrows():
        phone_number = standardize_phone_number(row.get("Contact Number"))
        remarks = row.get("Remarks")

        if pd.isna(phone_number) or not phone_number or pd.isna(remarks) or not remarks.strip():
            continue  # Skip if phone number or remarks are empty

        customer = existing_customers_numbers.get(phone_number)
        if customer:
            memory_entries.append({
                "customer_id": customer['customer_id'],
                "content": remarks.strip(),
                "source": "spreadsheet",
                "created_at": datetime.now().isoformat()
            })

    # Insert into 'memory' table
    if memory_entries:
        if logger:
            logger(f"Inserting {len(memory_entries)} new memory entries")
        supabase.table(get_table("memory", use_test_tables)).insert(memory_entries).execute()


def process_customer_data(file_path, disable_test_customer_data=False, logger=None):
    use_test_tables = not disable_test_customer_data
    dataframe = get_spreadsheet_data(file_path)
    
    
    # We must first create or update all customers

    # We must first create or update all customers
    validate_spreadsheet_columns(dataframe, "customer_details")
    logger and logger("✅ Step 1: Processing customer details")
    process_customer_details(dataframe, use_test_tables, logger)

    logger and logger("✅ Step 2: Processing order mappings")
    process_order_mappings(dataframe, use_test_tables, logger)

    validate_spreadsheet_columns(dataframe, "feedback")
    logger and logger("✅ Step 3: Processing feedback")
    process_feedback(dataframe, use_test_tables, logger)

    logger and logger("✅ Step 4: Processing memory entries")
    process_memory_entries(dataframe, use_test_tables, logger)

    refresh_views_analytics()

    logger and logger("🎉 All steps completed successfully")


