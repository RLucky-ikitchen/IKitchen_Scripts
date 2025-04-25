import streamlit as st
from src.data_import.servquick_pos_data import process_pos_data 
from src.data_import.new_customer_data import process_customer_data
from src.data_import.openai_business_card_parsing import process_all_business_cards
from src.data_import.process_ivr_audio import process_audio_files
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
    uploaded_file = st.file_uploader("Choose a file", type=["xls", "csv"], key="pos_file")

    disable_test_pos_data = st.toggle("Disable Test Mode", key='POS data test')

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
                    log_buffer.write(message + "\n")
                    log_placeholder.text(log_buffer.getvalue())


                with st.spinner("Processing the uploaded file..."):
                    process_pos_data(temp_file_path, disable_test_pos_data, logger=log_function)

                st.success("File processed and data inserted into Supabase successfully!")


            except Exception as e:
                st.error(f"An error occurred while processing the file: {e}")
            os.remove(temp_file_path)
        else:
            st.warning("Please upload a file before clicking the 'Process File' button.")


st.header("Customer Data")
with st.expander("Import Data"):
    st.markdown("""
1. Access the [Customer Data Spreadsheet](https://docs.google.com/spreadsheets/d/1NoUYJkeKRGx5XRI18geTr5km9QZhUwKJcgVva5wcRtE/edit?gid=0#gid=0)
2. Select the desired month/year and download as .csv (Do not download as .xlsx !)
""")
    uploaded_file = st.file_uploader("Choose a file", type=["csv"], key="customer_file")

    disable_test_customer_data = st.toggle("Disable Test Mode", key='customer data test')

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
                    log_buffer.write(message + "\n")
                    log_placeholder.text(log_buffer.getvalue())

                
                with st.spinner("Processing the uploaded file..."):
                    process_customer_data(temp_file_path, disable_test_customer_data, logger=log_function)

                st.success("File processed and data inserted into Supabase successfully!")

            except Exception as e:
                # Handle exceptions and display the error message
                st.error(f"An error occurred while processing the file: {e}")
            os.remove(temp_file_path)
        else:
            st.warning("Please upload a file before clicking the 'Process File' button.")


st.header("Business Card Import")
with st.expander("Import Data"):
    st.markdown("""
1. Access the [Business Card Scanner Folder](https://drive.google.com/drive/folders/1L_XK2i2OJoncZLk2M2i64TzEZ9Ro3LLe)
2. Download the images from the **New JPEG** folder
3. After uploading, move them to the **Old JPEG** folder in Drive to avoid duplicate processing
4. Upload the downloaded images below
""")
    
    uploaded_files = st.file_uploader("Upload business card images", 
                                      type=["jpg", "jpeg", "png"], 
                                      accept_multiple_files=True,
                                      key="business_card_files")

    disable_test_business_card = st.toggle("Disable Test Mode", key='business card test')

    # Button to process the business cards
    if st.button("Process Business Cards", key='business card process'):
        if uploaded_files and len(uploaded_files) > 0:
            try:
                log_buffer = StringIO()
                log_placeholder = st.empty()  # Placeholder for real-time logs

                def log_function(message):
                    """Append message to the log buffer and update Streamlit UI."""
                    log_buffer.write(message + "\n")
                    log_placeholder.text(log_buffer.getvalue())

                with st.spinner("Processing business card images..."):
                    process_all_business_cards(
                        uploaded_files, 
                        test_mode=not disable_test_business_card,
                        logger=log_function
                    )

                st.success(f"Processed {len(uploaded_files)} business cards and updated database!")

            except Exception as e:
                st.error(f"An error occurred while processing business cards: {e}")
        else:
            st.warning("Please upload at least one business card image before processing.")


st.header("IVR Audio Import")
with st.expander("Import Data"):
    st.markdown("""
    1. Access the [IVR audio recordings](https://drive.google.com/drive/folders/1XsSeJVCZts1ERFunr8TNt5GCF-A7iChi)
    2. Download the audio files that you want to process
    3. Upload the downloaded files below
                """)
    uploaded_files = st.file_uploader("Upload IVR audio files", type=["mp3"], accept_multiple_files=True, key="ivr_audio_files")
    disable_test_ivr_audio = st.toggle("Disable Test Mode", key='IVR audio test')
    # Button to process the IVR audio files
    if st.button("Process IVR Audio Files", key='IVR audio process'):
        if uploaded_files and len(uploaded_files) > 0:
            try:
                log_buffer = StringIO()
                log_placeholder = st.empty()  # Placeholder for real-time logs

                def log_function(message):
                    """Append message to the log buffer and update Streamlit UI."""
                    log_buffer.write(message + "\n")
                    log_placeholder.text(log_buffer.getvalue())

                with st.spinner("Processing IVR audio files..."):
                    process_audio_files(
                        uploaded_files, 
                        test_mode=not disable_test_ivr_audio,
                        logger=log_function
                    )

                st.success(f"Processed {len(uploaded_files)} IVR audio files and updated database!")

            except Exception as e:
                st.error(f"An error occurred while processing IVR audio files: {e}")
        else:
            st.warning("Please upload at least one IVR audio file before processing.")

st.header("Reset all Testing data")
if st.button("Reset", key='test data reset'):
    with st.spinner("Deleting all test data from Supabase ..."):
        reset_test_data()
    st.success("Done !")