import requests
import pandas as pd
from google.cloud import bigquery
import os
from datetime import datetime, timedelta

# --- Authentication and Configuration ---
# Replace with the path to your service account key file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\mleese5\OneDrive\OneDrive - JLR\98.Python\ETF dashboard\data to bq\ml8849-902e0838d83c.json'

# Replace with your Google Cloud Project ID
PROJECT_ID = 'ml8849'
# Replace with your BigQuery dataset ID
DATASET_ID = 'trading_dashboard'
# Replace with your BigQuery table ID
TABLE_ID = 'historic_data'

# Get your free Alpha Vantage API key from https://www.alphavantage.co/support/#api-key
ALPHA_VANTAGE_API_KEY = 'E3AHZ2K13ICQR8FC'  # <--- **REPLACE WITH YOUR API KEY**

# List of ticker symbols to download
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']

# Specify the date range for historical data
# Alpha Vantage free tier often requires the entire history for "outputsize=full"
# We'll filter the data in Pandas after download.
# For demonstration, we'll still use the date range, but the API call might
# fetch more data than needed depending on the outputsize.
START_DATE_STR = '2022-01-01'  # Use string format for filtering
END_DATE_STR = '2023-12-31'

# Convert date strings to datetime objects for filtering
START_DATE = datetime.strptime(START_DATE_STR, '%Y-%m-%d').date()
END_DATE = datetime.strptime(END_DATE_STR, '%Y-%m-%d').date()

# --- Helper function to create BigQuery table if it doesn't exist ---
def create_bigquery_table(client, dataset_id, table_id):
    """Creates a BigQuery table if it doesn't exist."""
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
        print(f"Table {dataset_id}.{table_id} already exists.")
    except Exception as e:
        print(f"Table {dataset_id}.{table_id} not found. Creating table.")
        schema = [
            bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("Open", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("High", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("Low", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("Close", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("Adj_Close", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("Volume", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Ticker", "STRING", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {dataset_id}.{table_id}")

# --- Data Download Function (using Alpha Vantage) ---
def download_stock_data_alpha_vantage(ticker, api_key, start_date, end_date):
    """Downloads historical stock data for a single ticker from Alpha Vantage."""
    base_url = "https://www.alphavantage.co/query?"
    function = "TIME_SERIES_DAILY_ADJUSTED"  # Provides adjusted close
    # outputsize='compact' gets the last 100 data points
    # outputsize='full' gets the full history (subject to API limits and plan)
    outputsize = 'full'

    params = {
        "function": function,
        "symbol": ticker,
        "outputsize": outputsize,
        "apikey": api_key
    }

    try:
        print(f"Attempting to download data for {ticker} from Alpha Vantage...")
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()

        # Check for API errors or if data is not available
        if "Error Message" in data:
            print(f"Alpha Vantage API error for {ticker}: {data['Error Message']}")
            return pd.DataFrame()  # Return empty DataFrame on error
        if f"Time Series (Daily)" not in data:
             print(f"No daily time series data found for {ticker}.")
             return pd.DataFrame() # Return empty DataFrame if data is missing

        time_series_data = data[f"Time Series (Daily)"]

        # Convert the JSON data to a Pandas DataFrame
        df = pd.DataFrame.from_dict(time_series_data, orient='index')
        df.index.name = 'Date'

        # Rename columns to be more descriptive and BigQuery friendly
        df.rename(columns={
            '1. open': 'Open',
            '2. high': 'High',
            '3. low': 'Low',
            '4. close': 'Close',
            '5. adjusted close': 'Adj_Close',
            '6. volume': 'Volume',
            '7. dividend amount': 'Dividend_Amount', # Keep for completeness, can drop if not needed
            '8. split coefficient': 'Split_Coefficient' # Keep for completeness, can drop if not needed
        }, inplace=True)

        # Convert index to datetime and filter by date range
        df.index = pd.to_datetime(df.index)
        df_filtered = df[(df.index >= start_date) & (df.index <= end_date)].copy()

        if df_filtered.empty:
            print(f"No data found for {ticker} within the specified date range ({start_date} to {end_date}).")
            return pd.DataFrame() # Return empty DataFrame if no data in range

        # Add the 'Ticker' column
        df_filtered['Ticker'] = ticker

        # Reset index to make 'Date' a regular column
        df_filtered.reset_index(inplace=True)

        # Ensure 'Date' column is in the correct format for BigQuery DATE type
        df_filtered['Date'] = pd.to_datetime(df_filtered['Date']).dt.date

        # Convert column types to match BigQuery schema
        for col in ['Open', 'High', 'Low', 'Close', 'Adj_Close']:
            df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').astype(float)
        df_filtered['Volume'] = pd.to_numeric(df_filtered['Volume'], errors='coerce').astype('Int64') # Use nullable integer type

        # Drop columns not needed for BigQuery (optional)
        df_filtered.drop(columns=['Dividend_Amount', 'Split_Coefficient'], errors='ignore', inplace=True)

        return df_filtered

    except requests.exceptions.RequestException as e:
        print(f"Error during Alpha Vantage API call for {ticker}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred processing data for {ticker}: {e}")
        return pd.DataFrame()

# --- BigQuery Upload Function ---
def upload_data_to_bigquery(data_df, project_id, dataset_id, table_id):
    """Uploads a Pandas DataFrame to a BigQuery table."""
    if data_df.empty:
        print("No data to upload to BigQuery.")
        return

    bigquery_client = bigquery.Client(project=project_id)

    # Create the BigQuery table if it doesn't exist (called here or before collecting data)
    # create_bigquery_table(bigquery_client, dataset_id, table_id)

    print(f"Uploading {len(data_df)} rows to BigQuery...")
    try:
        job = bigquery_client.load_table_from_dataframe(
            data_df,
            f"{dataset_id}.{table_id}",
            job_config=bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
                    bigquery.SchemaField("Open", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("High", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("Low", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("Close", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("Adj_Close", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("Volume", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("Ticker", "STRING", mode="REQUIRED"),
                ],
                write_disposition="WRITE_APPEND",  # Append new data
            )
        )
        job.result()  # Wait for the job to complete
        print(f"Successfully uploaded {len(data_df)} rows to {dataset_id}.{table_id}.")

    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")

# --- Main Execution Flow ---
if __name__ == "__main__":
    # Ensure your GCP credentials are set up
    # The os.environ line should handle this if the path is correct.

    if not PROJECT_ID or PROJECT_ID == 'your-gcp-project-id':
        print("Please update PROJECT_ID with your actual Google Cloud Project ID.")
    elif not DATASET_ID or DATASET_ID == 'your_bigquery_dataset_id':
        print("Please update DATASET_ID with your actual BigQuery dataset ID.")
    elif not TABLE_ID or TABLE_ID == 'your_bigquery_table_id':
        print("Please update TABLE_ID with your actual BigQuery table ID.")
    elif not ALPHA_VANTAGE_API_KEY or ALPHA_VANTAGE_API_KEY == 'YOUR_ALPHA_VANTAGE_API_KEY':
         print("Please update ALPHA_VANTAGE_API_KEY with your actual Alpha Vantage API key.")
    else:
        all_stock_data = pd.DataFrame()

        # Create BigQuery client once
        bigquery_client = bigquery.Client(project=PROJECT_ID)
        # Create the table before processing any data
        create_bigquery_table(bigquery_client, DATASET_ID, TABLE_ID)

        for ticker in TICKERS:
            stock_data = download_stock_data_alpha_vantage(ticker, ALPHA_VANTAGE_API_KEY, START_DATE, END_DATE)
            if not stock_data.empty:
                all_stock_data = pd.concat([all_stock_data, stock_data], ignore_index=True)

        # Upload all collected data after downloading for all tickers
        if not all_stock_data.empty:
            print(len(all_stock_data))
            print(all_stock_data.columns)
            print(all_stock_data.head(50))
            upload_data_to_bigquery(all_stock_data, PROJECT_ID, DATASET_ID, TABLE_ID)
        else:
             print("No data was successfully downloaded for any ticker.")
