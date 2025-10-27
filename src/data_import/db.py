import os
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import List, Dict

from src.models import Order

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
    "feedback": "feedback",
    "memory": "memory",
    'ivr_transcripts': 'ivr_transcripts',
    "transactions": "transactions",
}

TEST_TABLES = {
    "customers": "customers_testing",
    "orders": "orders_testing",
    "feedback": "feedback_testing",
    "memory": "memory_testing",
    'ivr_transcripts': 'ivr_transcripts_testing',
    "transactions": "transactions_testing",
}


BATCH_SIZE = 1000

def get_table(name: str, testing: bool):
    if testing:
        return TEST_TABLES.get(name)
    else:
        return PROD_TABLES.get(name)


def reset_test_data():
    supabase.table('memory_testing').delete().neq("customer_id", "00000000-0000-0000-0000-000000000000").execute()
    supabase.table('feedback_testing').delete().neq("customer_id", "00000000-0000-0000-0000-000000000000").execute()
    supabase.table('orders_testing').delete().neq("receipt_id", "").execute()
    supabase.table('customers_testing').delete().neq("customer_id", "00000000-0000-0000-0000-000000000000").execute()


def get_existing_customers(phone_numbers: List[str], use_test_tables: bool) -> Dict[str, dict]:
    existing_customers_data = supabase.table(get_table("customers", use_test_tables)).select("*").in_("phone_number", phone_numbers).execute()
    return {cust['phone_number']: cust for cust in existing_customers_data.data}

def get_existing_feedback(customer_ids: List[str], use_test_tables: bool, batch_size: int = 100) -> Dict[str, dict]:
    existing_feedback = {}
    table = supabase.table(get_table("feedback", use_test_tables))

    for i in range(0, len(customer_ids), batch_size):
        batch = customer_ids[i:i + batch_size]
        try:
            response = table.select("feedback_id", "customer_id").in_("customer_id", batch).execute()
            if response.data:
                for fb in response.data:
                    existing_feedback[fb["customer_id"]] = fb
        except Exception as e:
            print(f"Error fetching feedback batch {i}-{i + len(batch)}: {e}")
            raise

    return existing_feedback


def get_existing_orders(receipt_numbers: List[str], use_test_tables: bool) -> Dict[str, dict]:
    existing_orders_data = supabase.table(get_table("orders", use_test_tables)).select("*").in_("receipt_id", receipt_numbers).execute()
    return {order['receipt_id']: order for order in existing_orders_data.data}


def get_existing_receipts_ids(receipt_numbers: List[str], use_test_tables: bool, batch_size: int = 100):
    existing_receipts = set()
    table = supabase.table(get_table("orders", use_test_tables))

    for i in range(0, len(receipt_numbers), batch_size):
        batch = receipt_numbers[i:i + batch_size]
        try:
            response = table.select("*").in_("receipt_id", batch).execute()
            if response.data:
                existing_receipts.update([item["receipt_id"] for item in response.data])
        except Exception as e:
            print(f"Error fetching receipts batch {i}-{i+len(batch)}: {e}")
            continue  # Or raise if you want to stop on error

    return existing_receipts



def batch_insert_orders(orders: List[Order], use_test_tables):
    for i in range(0, len(orders), BATCH_SIZE):
        batch = [order.model_dump() for order in orders[i:i + BATCH_SIZE]]
        supabase.table(get_table("orders", use_test_tables)).insert(batch).execute()
