import os
import pandas as pd
import uuid
import argparse
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from utils import standardize_phone_number

# Load environment variables
load_dotenv(".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials are missing. Check your .env file.")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Function to insert unique customers
def insert_customer(customer):
    # Standardize phone number
    customer["phone_number"] = standardize_phone_number(customer["phone_number"])

    # Check if the customer already exists
    existing_customer = supabase.table("customers").select("*").eq("phone_number", customer["phone_number"]).execute()
    if not existing_customer.data:
        customer_id = str(uuid.uuid4())  # Generate a unique ID for the customer
        customer["customer_id"] = customer_id

        # Ensure all values in the customer dictionary are valid JSON types
        for key, value in customer.items():
            if pd.isnull(value):  # Check for NaN values
                customer[key] = None  # Replace NaN with None
            elif isinstance(value, float) and (pd.isna(value) or value == float('inf') or value == float('-inf')):
                customer[key] = None  # Replace infinite or NaN floats with None

        supabase.table("customers").insert(customer).execute()
        return customer_id
    return existing_customer.data[0]["customer_id"]

# Function to insert unique orders
def insert_order(order):
    supabase.table("orders").insert(order).execute()

def process_excel(file_path):
    # Read the Excel file
    data = pd.read_excel(file_path)

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
    data["Item amount"] = pd.to_numeric(data["Item amount"], errors="coerce")

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
    for _, row in final_data.iterrows():
        # Extract customer details
        customer = {
            "name": row[column_map["Customer name"]],
            "phone_number": row[column_map["Customer mobile"]],
            "email": row[column_map["Customer email"]],
            "address": row[column_map["Customer address"]],
        }

        # Insert the customer and retrieve their ID
        customer_id = insert_customer(customer)

        # Skip if customer insertion fails
        if not customer_id:
            continue

        # Get and validate the order type
        order_type = row[column_map["Ordertype name"]]
        if order_type not in order_type_map:
            print(f"Skipping order with invalid order type: {order_type}")
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
            insert_order(order)
        except Exception as e:
            print(f"Failed to insert order: {e}")

    print("Data successfully inserted into Supabase.")

# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and insert data into Supabase.")
    parser.add_argument("file_path", type=str, help="Path to the Excel file containing the data.")
    args = parser.parse_args()

    # Call the process_excel function with the provided file path
    process_excel(args.file_path)
