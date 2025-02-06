import pandas as pd
import uuid
import argparse

from src.data_import.db import supabase, get_table
from src.utils import standardize_phone_number, get_spreadsheet_data


# Function to insert unique customers
def insert_customer(customer, test_tables=False):
    # Standardize phone number
    customer["phone_number"] = standardize_phone_number(customer["phone_number"])

    # Check if the customer already exists
    existing_customer = supabase.table(get_table("customers", test_tables)).select("*").eq("phone_number", customer["phone_number"]).execute()
    if not existing_customer.data:
        customer_id = str(uuid.uuid4())  # Generate a unique ID for the customer
        customer["customer_id"] = customer_id

        # Ensure all values in the customer dictionary are valid JSON types
        for key, value in customer.items():
            if pd.isnull(value):  # Check for NaN values
                customer[key] = None  # Replace NaN with None
            elif isinstance(value, float) and (pd.isna(value) or value == float('inf') or value == float('-inf')):
                customer[key] = None  # Replace infinite or NaN floats with None

        supabase.table(get_table("customers", test_tables)).insert(customer).execute()
        return customer_id
    return existing_customer.data[0]["customer_id"]


def process_pos_data(file_path, test_tables=False, logger=None):
    data = get_spreadsheet_data(file_path)

    data = data.dropna(subset=["Receipt no"])

    column_map = {
        "Customer name": "Customer name",
        "Customer mobile": "Customer mobile",
        "Customer email": "Customer email",
        "Customer address": "Customer address",
        "Sale date": "Sale date",
        "Receipt no": "Receipt no",  # Used for grouping and stored as ServQuick Receipt ID
        "Ordertype name": "Ordertype name",
        "Item name": "Item name",
        "Variant name": "Variant name",
        "Selling price": "Selling price",
        "Item quantity": "Item quantity",
        "Item amount": "Item amount",
    }

    # Ensure the necessary columns exist
    required_columns = column_map.keys()
    for col in required_columns:
        if col not in data.columns:
            raise ValueError(f"Missing required column: {col}")

    # Convert numeric columns to proper types
    data["Item quantity"] = pd.to_numeric(data["Item quantity"], errors="coerce")
    data["Item amount"] = data["Item amount"].str.replace(",", "")
    data["Item amount"] = pd.to_numeric(data["Item amount"], errors="coerce")

    if data["Item amount"].isna().any():
        logger("Some rows have invalid 'Item amount':")
        logger(data[data["Item amount"].isna()])

    # Group items by receipt number (used only for grouping here)
    grouped = data.groupby("Receipt no").apply(lambda group: {
        "order_items": group.apply(lambda row: {
            "item_name": row["Item name"],
            "quantity": row["Item quantity"],
            "amount": row["Item amount"],
        }, axis=1).tolist(),
        "order_items_text": "; ".join(
            f'{row["Item name"]} (x{row["Item quantity"]})' for _, row in group.iterrows()
        ),
    }).reset_index(name="grouped_data")

    # Extract grouped data into a DataFrame
    grouped_data = pd.json_normalize(grouped["grouped_data"])
    grouped = pd.concat([grouped, grouped_data], axis=1)

    # Merge grouped data back with the original dataset
    final_data = pd.merge(data.drop_duplicates("Receipt no"), grouped, on="Receipt no", how="left")

    # Define a mapping for valid order types
    order_type_map = {
        "Dine-In": "Dine-In",
        "Delivery": "Delivery",
        "Takeaway": "Take away",  # Map "Takeaway" to "Take away"
        "Take away": "Take away",  # Allow direct match
        "Eat in": "Dine-In",       # Handle alternate naming
    }

    # Process customers and orders
    for i, row in final_data.iterrows():
        if logger and i % 50 == 0:
            logger(f"Processed {i} receipts")
            
        # Extract customer details
        customer = {
            "name": row[column_map["Customer name"]],
            "phone_number": row[column_map["Customer mobile"]],
            "email": row[column_map["Customer email"]],
            "address": row[column_map["Customer address"]],
        }

        # Insert the customer and retrieve their ID
        customer_id = insert_customer(customer, test_tables)

        # Get and validate the order type
        order_type = row[column_map["Ordertype name"]]
        if order_type not in order_type_map:
            if logger:
                logger(f"Skipping order with invalid order type: {order_type}")
            continue

        # Prepare order details
        order = {
            "order_id": str(uuid.uuid4()),  # Generate a unique ID for the order
            "customer_id": customer_id,
            "order_date": row[column_map["Sale date"]].isoformat()
            if isinstance(row[column_map["Sale date"]], pd.Timestamp)
            else str(row[column_map["Sale date"]]),
            "order_items": row["order_items"],
            "order_items_text": row["order_items_text"],
            "total_amount": sum(item["amount"] for item in row["order_items"]),
            "order_type": order_type_map[order_type],  # Map to a valid enum type
            "receipt_id": row[column_map["Receipt no"]],  # ServQuick receipt ID
        }

        # Insert the order
        try:
            supabase.table(get_table("orders", test_tables)).insert(order).execute()
        except Exception as e:
            if logger:
                logger(f"Failed to insert order: {e}")

    if logger:
        logger(f"Processing complete. {len(final_data)} receipts processed.")


# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and insert data into Supabase.")
    parser.add_argument("file_path", type=str, help="Path to the Excel file containing the data.")
    args = parser.parse_args()

    # Call the process_excel function with the provided file path
    process_pos_data(args.file_path)
