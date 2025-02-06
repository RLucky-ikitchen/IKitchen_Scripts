import pandas as pd
import uuid

from typing import List, Dict
from src.models import Customer, Order, OrderItem

from src.data_import.db import supabase, get_table, BATCH_SIZE
from src.utils import standardize_phone_number, get_spreadsheet_data, validate_spreadsheet_columns


order_type_mapping = {
    "Take away": "Take away",
    "Eat in": "Dine-In",
    "Delivery": "Delivery"
}


def get_existing_receipt_ids(receipt_ids: List[str], use_test_tables) -> set:
    existing_receipts = set()
    for i in range(0, len(receipt_ids), BATCH_SIZE):
        batch_receipts = receipt_ids[i:i + BATCH_SIZE]
        response = supabase.table(get_table("orders", use_test_tables)) \
            .select("receipt_id") \
            .in_("receipt_id", batch_receipts) \
            .execute()
        existing_receipts.update([item["receipt_id"] for item in response.data])
    return existing_receipts


def batch_insert_customers(customers: List[Customer], use_test_tables) -> Dict[str, str]:
    customer_id_map = {}
    existing_customers = {}

    # Lookup existing customers
    phone_numbers = [customer.phone_number for customer in customers]
    for i in range(0, len(phone_numbers), BATCH_SIZE):
        batch_numbers = phone_numbers[i:i + BATCH_SIZE]
        response = supabase.table(get_table("customers", use_test_tables)) \
            .select("customer_id, phone_number") \
            .in_("phone_number", batch_numbers) \
            .execute()

        for customer in response.data:
            existing_customers[customer["phone_number"]] = customer["customer_id"]

    # Insert new customers
    new_customers = [
        customer for customer in customers 
        if customer.phone_number not in existing_customers
    ]

    for customer in new_customers:
        customer.customer_id = str(uuid.uuid4())
        customer_id_map[customer.phone_number] = customer.customer_id

    for i in range(0, len(new_customers), BATCH_SIZE):
        batch = [customer.model_dump() for customer in new_customers[i:i + BATCH_SIZE]]
        supabase.table(get_table("customers", use_test_tables)).insert(batch).execute()

    # Combine existing and new customer IDs
    customer_id_map.update(existing_customers)
    return customer_id_map


def batch_insert_orders(orders: List[Order], use_test_tables):
    for i in range(0, len(orders), BATCH_SIZE):
        batch = [order.model_dump() for order in orders[i:i + BATCH_SIZE]]
        supabase.table(get_table("orders", use_test_tables)).insert(batch).execute()


def process_pos_data(file_path, disable_test_pos_data=False, logger=None):

    use_test_tables = not disable_test_pos_data

    data = get_spreadsheet_data(file_path)
    validate_spreadsheet_columns(data, "servquick_columns")

    data = data.dropna(subset=["Receipt no"])

    # Data Cleaning
    data["Item quantity"] = pd.to_numeric(data["Item quantity"], errors="coerce")
    data["Item amount"] = data["Item amount"].astype(str).str.replace(",", "")
    data["Item amount"] = pd.to_numeric(data["Item amount"], errors="coerce")

    # Handle any invalid data
    if data["Item amount"].isna().any():
        if logger:
            logger("Invalid 'Item amount' detected:")
            logger(data[data["Item amount"].isna()])

    # Group Items by Receipt Number
    grouped = data.groupby("Receipt no").apply(lambda group: {
        "order_items": group.apply(lambda row: OrderItem(
            item_name=row["Item name"],
            quantity=row["Item quantity"],
            amount=row["Item amount"]
        ), axis=1).tolist(),
        "order_items_text": "; ".join(
            f'{row["Item name"]} (x{row["Item quantity"]})' for _, row in group.iterrows()
        )
    }).reset_index(name="grouped_data")

    final_data = pd.merge(data.drop_duplicates("Receipt no"), grouped, on="Receipt no", how="left")
    if logger:
        logger(f"Processing {len(final_data)} receipts...")
    
    # Logging time frame of the receipts
    if not final_data["Sale date"].isna().all():
        final_data["Sale date"] = pd.to_datetime(final_data["Sale date"], errors='coerce')
        
        min_date = final_data["Sale date"].min()
        max_date = final_data["Sale date"].max()
        
        if logger and pd.notna(min_date) and pd.notna(max_date):
            logger(f"Processing receipts from {min_date.strftime('%d/%m/%Y')} to {max_date.strftime('%d/%m/%Y')}")


    # Process all Customers
    customers = []
    for _, row in final_data.iterrows():
        phone_number = standardize_phone_number(row.get("Customer mobile"))
        if pd.isna(phone_number) or not phone_number:
            continue  # Skip if no phone number

        email = row.get("Customer email")
        address = row.get("Customer address")

        customer = Customer(
            name=row.get("Customer name"),
            phone_number=phone_number,
            email=email if not pd.isna(email) else None,
            address=address if not pd.isna(address) else None
        )
        customers.append(customer)

    customer_id_map = batch_insert_customers(customers, use_test_tables)
    if logger:
        logger(f"Processing {len(customers)} customers ...")

    # Process all Orders

    # First, fetch existing receipt IDs from the database
    receipt_ids = final_data["Receipt no"].unique().tolist()
    existing_receipt_ids = get_existing_receipt_ids(receipt_ids, use_test_tables)

    orders = []
    for _, row in final_data.iterrows():
        receipt_id = row['Receipt no']
        if receipt_id in existing_receipt_ids:
            if logger:
                logger(f"Skipping order with receipt ID: {receipt_id} - already in the database")
            continue  # Skip if the receipt already exists

        customer_id = customer_id_map.get(standardize_phone_number(row["Customer mobile"]))
        if not customer_id:
            continue # Skip if no customer ID

        order = Order(
            order_id=str(uuid.uuid4()),
            customer_id=customer_id,
            order_date=row["Sale date"].isoformat() if isinstance(row["Sale date"], pd.Timestamp) else str(row["Sale date"]),
            order_items=row['grouped_data']["order_items"],
            order_items_text=row['grouped_data']["order_items_text"],
            total_amount=sum(item.amount for item in row['grouped_data']["order_items"]),
            order_type=order_type_mapping.get(row["Ordertype name"], "Take away"),  # Default to "Take away" if not found
            receipt_id=row["Receipt no"]
        )
        orders.append(order)

    batch_insert_orders(orders, use_test_tables)

    if logger:
        logger(f"Processing complete. {len(final_data)} receipts processed.")
