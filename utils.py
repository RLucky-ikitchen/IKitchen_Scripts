import pandas as pd
import logging
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
            
    # Remove the country code and check the length
    local_number = phone_number[3:]  # Exclude the '880' country code
    if len(local_number) < 8 or len(local_number) > 11:
        logging.warning(f"Invalid phone number length for: {phone_number}")
        return None # Remove the number if it doesn't meet the length requirement

    return f"+{phone_number}"  # Add the '+' prefix

def convert_rating(value):
    """
    Convert rating strings to integers.
    """
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

def is_valid_email(email):
    """
    Check if an email is valid (simple validation to exclude placeholders).
    """
    return email not in ["-", "--", "---", None, ""] and not pd.isna(email)
