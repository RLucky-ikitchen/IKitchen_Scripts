# Setup Instructions

## First Time Setup
1. **Create Virtual Environment:**  
   ```bash
   python -m venv venv
   ```

2. **Activate Virtual Environment:**  
   - **Windows:**
     ```bash
     venv\Scripts\activate
     ```
   - **macOS/Linux:**
     ```bash
     source venv/bin/activate
     ```

3. **Install Dependencies:**  
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Configuration:**  
   - Copy the example environment file:
     ```bash
     cp example.env .env
     ```
   - Add your API keys and other necessary configurations to the `.env` file.

## Development
Run the following command to start the application:
```bash
streamlit run data_import_console.py
```

- **Accessible Local URL:** [http://localhost:8501](http://localhost:8501)

