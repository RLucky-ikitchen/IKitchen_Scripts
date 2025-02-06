import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials are missing. Check your .env file.")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

PROD_TABLES = {
    "customers": "customers",
    "orders": "orders",
    "feedback": "feedback"
}

TEST_TABLES = {
    "customers": "customers_testing",
    "orders": "orders_testing",
    "feedback": "feedback_testing"
}

def get_table(name: str, testing: bool):
    if testing:
        return TEST_TABLES.get(name)
    else:
        return PROD_TABLES.get(name)


def reset_test_data():
    supabase.table('feedback_testing').delete().neq("customer_id", "00000000-0000-0000-0000-000000000000").execute()
    supabase.table('orders_testing').delete().neq("customer_id", "00000000-0000-0000-0000-000000000000").execute()
    supabase.table('customers_testing').delete().neq("customer_id", "00000000-0000-0000-0000-000000000000").execute()
