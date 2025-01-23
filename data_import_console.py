import streamlit as st
from src.data_import.servquick_pos_data import process_file 
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
4. Use Advanced filters: Filter data by “Customer Name” is not empty OR “Customer mobile is not empty.
5. Export as csv or xls
""")
    uploaded_file = st.file_uploader("Choose a file", type=["xls", "csv"])

    # Button to process the file
    if st.button("Process File"):
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
                    process_file(temp_file_path, logger=log_function)

                # Notify the user of success
                st.success("File processed and data inserted into Supabase successfully!")

                # Optionally, delete the temporary file
                os.remove(temp_file_path)

            except Exception as e:
                # Handle exceptions and display the error message
                st.error(f"An error occurred while processing the file: {e}")
        else:
            st.warning("Please upload a file before clicking the 'Process File' button.")
