import os
import re
import base64
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.io as pio
from dotenv import load_dotenv
# Use langchain_openai (or langchain_community if preferred)
from langchain_openai import AzureChatOpenAI

# 1. Load environment variables
print("Loading environment variables...")
load_dotenv()

# 2. Retrieve Azure OpenAI credentials
api_key = os.getenv("OPENAI_API_KEY")
deployment_name = os.getenv("AZURE_DEPLOYMENT_NAME")
api_version = os.getenv("AZURE_API_VERSION")
azure_endpoint = os.getenv("AZURE_ENDPOINT")

# 3. Initialize Azure OpenAI
print("Initializing Azure OpenAI Model...")
try:
    llm = AzureChatOpenAI(
        openai_api_key=api_key,
        azure_endpoint=azure_endpoint,
        deployment_name=deployment_name,
        api_version=api_version,
        temperature=0.0
    )
    print("✅ Azure OpenAI Model initialized successfully!")
except Exception as e:
    print(f" Failed to Initialize Azure OpenAI Model: {e}")
    raise

# 4. Retrieve Snowflake credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def create_connection():
    """Create a Snowflake connection."""
    print("Creating Snowflake connection...")
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("✅ Snowflake connection established!")
        return conn
    except Exception as e:
        print(f"Snowflake Connection Failed: {e}")
        raise

def get_snowflake_metadata(conn):
    """Fetch Snowflake metadata."""
    print("Fetching Snowflake metadata...")
    metadata_query = """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(metadata_query)
        metadata_rows = cursor.fetchall()
        cursor.close()

        if not metadata_rows:
            raise ValueError("No metadata retrieved! Check permissions.")

        metadata_df = pd.DataFrame(metadata_rows, columns=["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"])
        metadata_dict = (
            metadata_df.drop(columns=["TABLE_NAME"])
            .groupby(metadata_df["TABLE_NAME"], group_keys=False)
            .apply(lambda x: {col: dtype for col, dtype in zip(x["COLUMN_NAME"], x["DATA_TYPE"])})
            .to_dict()
        )
        print("Metadata retrieved successfully!")
        return metadata_dict
    except Exception as e:
        print(f" Error fetching metadata: {str(e)}")
        return None

def query_snowflake(conn, sql_query):
    """Execute a SQL query in Snowflake."""
    print("Executing SQL query:")
    print(sql_query)
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()

        if not result:
            print("Query returned no data!")
            return pd.DataFrame()

        print("SQL query executed successfully!")
        return pd.DataFrame(result, columns=columns)
    except Exception as e:
        print(f" SQL Execution Error: {e}")
        return pd.DataFrame({"Error": [str(e)]})

def visual_generate(query, data, response):
    """
    Generate an interactive HTML chart from the query results.
    Returns an HTML string or an empty string if generation fails.
    """
    print("Attempting to generate interactive HTML visualization...")
    try:
        # Convert data to a DataFrame
        df = pd.DataFrame(data)
        # If no data or fewer than 2 columns, skip chart generation
        if df.empty or len(df.columns) < 2:
            print(" Not enough data to generate a chart.")
            return ""
        
        # Example: Generate a bar chart using Plotly Express
        fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=response)

        # Customize layout with your theme
        fig.update_layout(
            plot_bgcolor="#2B2C2E",
            paper_bgcolor="#2B2C2E",
            font=dict(color="#FFFFFF"),
        )

        # Generate an interactive HTML string for the chart (no Kaleido required)
        html_str = pio.to_html(fig, full_html=False)
        print(" HTML visualization generated successfully!")
        return html_str

    except Exception as e:
        print(f"HTML chart generation error: {e}")
        return ""

# Export llm so it can be imported in app.py
__all__ = ["create_connection", "get_snowflake_metadata", "query_snowflake", "visual_generate", "llm"]
