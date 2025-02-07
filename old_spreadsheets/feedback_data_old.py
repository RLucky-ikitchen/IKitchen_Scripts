from supabase import create_client, Client
from src.utils import standardize_phone_number, convert_rating
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import argparse
import os

# Load environment variables
load_dotenv(".env")

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def format_date(date, time):
    """Format date and time properly"""
    try:
        if pd.isna(time):
            dt = pd.to_datetime(date)
        else:
            dt = pd.to_datetime(f"{date} {time}")
        return dt.isoformat()
    except Exception as e:
        print(f"Date formatting error: {e}")
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
    """Insert feedback data for a customer, replacing old feedback if it exists"""
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

        # Skip empty feedback
        numeric_ratings = [
            feedback_data.get("food_review", 0),
            feedback_data.get("service", 0),
            feedback_data.get("cleanliness", 0),
            feedback_data.get("atmosphere", 0),
            feedback_data.get("value", 0),
            feedback_data.get("overall_experience", 0),
        ]
        if all(rating == 0 for rating in numeric_ratings):
            print(f"Skipping empty feedback for customer {customer_id}")
            return False

        # Check if feedback already exists for this customer
        existing_feedback = supabase.table("feedback").select("feedback_id").eq("customer_id", customer_id).execute()
        if existing_feedback.data:
            # Update existing feedback
            feedback_id = existing_feedback.data[0]["feedback_id"]
            supabase.table("feedback").update(feedback_data).eq("feedback_id", feedback_id).execute()
            print(f"Updated feedback for customer {customer_id}")
        else:
            # Insert new feedback
            supabase.table("feedback").insert(feedback_data).execute()
            print(f"Inserted new feedback for customer {customer_id}")

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
