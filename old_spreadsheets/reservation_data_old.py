from supabase import create_client, Client
from utils import standardize_phone_number
from dotenv import load_dotenv
import pandas as pd
import argparse
import os

# Load environment variables
load_dotenv(".env")

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_data(file_path, sheet_name):
    """Load data from an Excel file."""
    return pd.read_excel(file_path, sheet_name=sheet_name)

def clean_data(df):
    """Clean the data by standardizing column names and formatting values."""
    df.columns = df.columns.str.strip()
    df["Total Bill"] = pd.to_numeric(df["Total Bill"].str.replace(",", "").str.replace(" BDT", ""), errors='coerce')
    df["Company Name"] = df["Company Name"].fillna("")
    df["Mobile Number"] = df["Mobile Number"].astype(str).replace('nan', '')
    return df

def process_row(row):
    """Process a single row of the DataFrame."""
    raw_phone = row["Mobile Number"]
    phone_number = standardize_phone_number(raw_phone)

    if not phone_number:
        print(f"Skipping row with invalid phone number: {raw_phone}")
        return

    is_returning = row["Return Guest"] == "Yes"
    company_name = row["Company Name"] if ("family" not in row["Company Name"].lower() and "friend" not in row["Company Name"].lower()) else None

    # Check for existing customer
    response = supabase.table("customers").select("*").eq("phone_number", phone_number).execute()

    if response.data:
        # Update existing customer
        customer_id = response.data[0]["customer_id"]
        update_data = {"is_returning_customer": is_returning}
        if company_name:
            update_data["company_name"] = company_name

        supabase.table("customers").update(update_data).eq("customer_id", customer_id).execute()
    else:
        # Create new customer
        print(f"Creating new customer with phone number {phone_number}")
        supabase.table("customers").insert({
            "phone_number": phone_number,
            "name": row["Guest Name"],
            "company_name": company_name if company_name else "",
            "is_returning_customer": is_returning,
        }).execute()

def process_reservations(file_path, sheet_name):
    """Load, clean, and process reservation data."""
    df = load_data(file_path, sheet_name)
    df = clean_data(df)

    for _, row in df.iterrows():
        process_row(row)

    print("Update process completed!")

def main():
    """Main function to parse arguments and process reservations."""
    parser = argparse.ArgumentParser(description="Process reservation updates.")
    parser.add_argument("file_path", type=str, help="Path to the Excel file containing reservation data")
    parser.add_argument("sheet_name", type=str, help="Name of the sheet in the Excel file to process")
    args = parser.parse_args()

    process_reservations(args.file_path, args.sheet_name)

if __name__ == "__main__":
    main()
