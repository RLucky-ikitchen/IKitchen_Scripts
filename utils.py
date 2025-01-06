import pandas as pd
# Function to standardize phone numbers
def standardize_phone_number(phone_number):
    if pd.isna(phone_number):
        return None
    # Remove spaces, dashes, and other non-numeric characters
    phone_number = "".join(filter(str.isdigit, phone_number))

    # Add the country code +880 if not already present
    if not phone_number.startswith("+880"):
        if phone_number.startswith("0"):
            phone_number = "880" + phone_number[1:]  # Remove leading zero
        elif phone_number.startswith("880"):
            phone_number = phone_number
        else:
            phone_number = "880" + phone_number

    return f"+{phone_number}"  # Add the '+' prefix