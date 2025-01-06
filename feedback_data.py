import pandas as pd
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from datetime import datetime
from utils import standardize_phone_number
import argparse

# Load environment variables
load_dotenv(".env")

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def convert_rating(value):
    """Convert rating strings to integers"""
    rating_map = {
        'poor': 1,
        'fair': 2,
        'good': 3,
        'great': 4
    }

    if pd.isna(value):
        return 0  # Default to 0 for missing values

    cleaned_value = str(value).lower().strip()
    return rating_map.get(cleaned_value, 1)

def format_date(date, time):
    """Format date and time properly"""
    try:
        if pd.isna(time):
            dt = pd.to_datetime(date)
        else:
            dt = pd.to_datetime(f"{date} {time}")
        return dt.isoformat()
    except:
        return datetime.now().isoformat()

def get_or_create_customer(row):
    """Get existing customer ID or create new customer"""
    try:
        raw_phone = str(row["Phone Number"]).strip() if not pd.isna(row["Phone Number"]) else None
        phone_number = standardize_phone_number(raw_phone) if raw_phone else None
        email = str(row["Email"]).strip() if not pd.isna(row["Email"]) else None

        # Try to find existing customer by phone or email
        existing_customer = None
        if phone_number:
            phone_response = supabase.table("customers").select("customer_id").eq("phone_number", phone_number).execute()
            if phone_response.data:
                existing_customer = phone_response.data[0]

        if not existing_customer and email:
            email_response = supabase.table("customers").select("customer_id").eq("email", email).execute()
            if email_response.data:
                existing_customer = email_response.data[0]

        if existing_customer:
            return existing_customer["customer_id"]

        # Create new customer if not found
        customer_data = {
            "name": str(row["Customer Name"]).strip() if not pd.isna(row["Customer Name"]) else "",
            "email": email,
            "phone_number": phone_number,
            "address": str(row["Address"]).strip() if not pd.isna(row["Address"]) else ""
        }

        new_customer = supabase.table("customers").insert(customer_data).execute()
        return new_customer.data[0]["customer_id"]

    except Exception as e:
        print(f"Error in get_or_create_customer: {e}")
        return None

def insert_feedback(row):
    """Insert feedback data for a customer"""
    try:
        # Get or create customer
        customer_id = get_or_create_customer(row)
        if not customer_id:
            print("Could not get or create customer, skipping feedback")
            return False

        # Prepare feedback data
        feedback_data = {
            "customer_id": customer_id,
            "food_review": convert_rating(row.get("Food Review")),
            "service": convert_rating(row.get("Service")),
            "cleanliness": convert_rating(row.get("Cleanliness")),
            "atmosphere": convert_rating(row.get("Atmosphere")),
            "value": convert_rating(row.get("Value")),
            "overall_experience": convert_rating(row.get("Overall Experience")),
            "feedback_date": format_date(row['Date'], row.get('Time'))
        }

        # Insert feedback
        feedback_response = supabase.table("feedback").insert(feedback_data).execute()
        print(f"Successfully inserted feedback for customer {customer_id}")
        return True

    except Exception as e:
        print(f"Error inserting feedback: {e}")
        return False

def main(file_path):
    """Main function to process the feedback data"""
    try:
        # Load the feedback data from the Excel file
        feedback_df = pd.read_excel(file_path)

        total_rows = len(feedback_df)
        successful_inserts = 0
        failed_inserts = 0

        print(f"Processing {total_rows} rows...")

        # Process each row
        for index, row in feedback_df.iterrows():
            print(f"\nProcessing row {index + 1} of {total_rows}")
            if insert_feedback(row):
                successful_inserts += 1
            else:
                failed_inserts += 1

        print(f"\nImport completed!")
        print(f"Successfully inserted: {successful_inserts}")
        print(f"Failed to insert: {failed_inserts}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Use argparse to accept the Excel file path as an argument
    parser = argparse.ArgumentParser(description="Process feedback data from an Excel file.")
    parser.add_argument(
        "file_path",
        type=str,
        help="Path to the Excel file containing feedback data."
    )
    args = parser.parse_args()

    # Call the main function with the provided file path
    main(args.file_path)
