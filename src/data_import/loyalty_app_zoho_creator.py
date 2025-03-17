import requests
from pydantic import BaseModel
from typing import List, Optional

CLIENT_ID = "1000.D4360WHYLKFPH29OGJW33LB26WUJSP"
CLIENT_SECRET = "8d88406668268f4567e8b443225f051c7ddc95da28"

TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"
GRANT_TYPE = "client_credentials"
SCOPE = "ZohoCreator.report.READ"
params = {
       "client_id": CLIENT_ID,
       "client_secret": CLIENT_SECRET,
       "grant_type": GRANT_TYPE,
       "scope": SCOPE
}

def get_new_access_token():
    response = requests.post(TOKEN_URL, params=params)
    return response.json()


class Customer(BaseModel):
    name: str
    phone_number: str
    email: Optional[str]
    company_name: Optional[str]
    membership_tier: Optional[str]


def fetch_all_records():
    access_token = get_new_access_token().get('access_token')
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }
    
    account_name = 'romi_ikitchen'
    app_name = 'ikitchen-loyalty-program-management'
    report_name = 'All_Members'
    base_url = f"https://creator.zoho.com/api/v2/{account_name}/{app_name}/report/{report_name}"
    
    customers = []
    start_index = 0
    batch_size = 200
    
    while True:
        url = f"{base_url}?from={start_index}&limit={batch_size}"
        response = requests.get(url, headers=headers)
        data = response.json()
        
        if 'data' not in data or not data['data']:
            break  # Stop if there are no more records
        
        for record in data['data']:
            customer = Customer(
                name=record.get('Member', {}).get('display_value', 'Unknown'),
                phone_number=record.get('Mobile_Number', ''),
                email=record.get('Email', None),
                company_name=record.get('Company_Name', None),
                membership_tier=record.get('Membership_Tier1', None)
            )
            customers.append(customer)
        
        start_index += batch_size  # Move to the next batch
    
    return customers

if __name__ == "__main__":
    customers = fetch_all_records()
    for customer in customers:
        print(customer.json())