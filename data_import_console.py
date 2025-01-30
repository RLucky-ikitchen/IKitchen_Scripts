import streamlit as st
from src.data_import.servquick_pos_data import process_pos_data 
from src.data_import.new_customer_data import process_customer_data
from src.data_import.db import reset_test_data
import os
from io import StringIO


# Set up the Streamlit app
st.title("IKitchen Data Import Console")

st.header("ServQuick POS Data")
with st.expander("Import Data"):
    st.write("""
1. Access ServQuick Dashboard: https://ikitchenbdltd.servquick.com/servquick/
2. Got to Reports > Transaction Summary > Sales Details by receipt
3. Set the Time Frame (for example, last month)
4. Export as csv or xls
""")
    uploaded_file = st.file_uploader("Choose a file", type=["xls", "csv"])

    test_pos_data = st.toggle("Test Mode", key='POS data test')

    # Button to process the file
    if st.button("Process File", key='POS data process'):
        if uploaded_file is not None:
            try:
                # Determine the file type
                file_extension = os.path.splitext(uploaded_file.name)[1].lower()

                # Extract the original file name (without extension)
                original_file_name = os.path.splitext(uploaded_file.name)[0]

                # Construct a temporary file path including the original file name
                temp_file_path = os.path.join(f"temp_{original_file_name}{file_extension}")
                with open(temp_file_path, "wb") as temp_file:
                    temp_file.write(uploaded_file.getbuffer())

                log_buffer = StringIO()

                log_placeholder = st.empty()  # Placeholder for real-time logs

                def log_function(message):
                    """Append message to the log buffer and update Streamlit UI."""
                    log_buffer.write(message + "\\n")
                    log_placeholder.text(log_buffer.getvalue())

                # Pass the log function to your processing function
                with st.spinner("Processing the uploaded file..."):
                    process_pos_data(temp_file_path, test_pos_data, logger=log_function)

                # Notify the user of success
                st.success("File processed and data inserted into Supabase successfully!")

                # Optionally, delete the temporary file
                os.remove(temp_file_path)

            except Exception as e:
                # Handle exceptions and display the error message
                st.error(f"An error occurred while processing the file: {e}")
        else:
            st.warning("Please upload a file before clicking the 'Process File' button.")


st.header("Customer Data")
with st.expander("Import Data"):
    st.markdown("""
1. Access the [Customer Data Spreadsheet](https://docs.google.com/spreadsheets/d/1NoUYJkeKRGx5XRI18geTr5km9QZhUwKJcgVva5wcRtE/edit?gid=0#gid=0)
2. Select the desired month/year and download as .csv (Do not download as .xlsx !)
""")
    uploaded_file = st.file_uploader("Choose a file", type=["csv"])

    test_customer_data = st.toggle("Test Mode", key='customer data test')

    # Button to process the file
    if st.button("Process File", key='customer data process'):
        if uploaded_file is not None:
            try:
                # Determine the file type
                file_extension = os.path.splitext(uploaded_file.name)[1].lower()

                # Extract the original file name (without extension)
                original_file_name = os.path.splitext(uploaded_file.name)[0]

                # Construct a temporary file path including the original file name
                temp_file_path = os.path.join(f"temp_{original_file_name}{file_extension}")
                with open(temp_file_path, "wb") as temp_file:
                    temp_file.write(uploaded_file.getbuffer())

                log_buffer = StringIO()

                log_placeholder = st.empty()  # Placeholder for real-time logs

                def log_function(message):
                    """Append message to the log buffer and update Streamlit UI."""
                    log_buffer.write(message + "\\n")
                    log_placeholder.text(log_buffer.getvalue())

                # Pass the log function to your processing function
                with st.spinner("Processing the uploaded file..."):
                    process_customer_data(temp_file_path, test_customer_data, logger=log_function)

                # Notify the user of success
                st.success("File processed and data inserted into Supabase successfully!")

                # Optionally, delete the temporary file
                os.remove(temp_file_path)

            except Exception as e:
                # Handle exceptions and display the error message
                st.error(f"An error occurred while processing the file: {e}")
        else:
            st.warning("Please upload a file before clicking the 'Process File' button.")


st.header("Reset all Testing data")
if st.button("Reset", key='test data reset'):
    with st.spinner("Deleting all test data from Supabase ..."):
        reset_test_data()
    st.success("Done !")
