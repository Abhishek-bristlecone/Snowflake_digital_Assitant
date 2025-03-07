import os
import re
import base64
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.io as pio
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI  # Corrected import

# Load Environment Variables
print("🔹 Loading environment variables...")
load_dotenv()

# Retrieve Azure OpenAI credentials
api_key = os.getenv("OPENAI_API_KEY")
deployment_name = os.getenv("OPENAI_DEPLOYMENT_NAME")
api_version = os.getenv("OPENAI_API_VERSION")

# Check API credentials
if not api_key or not deployment_name or not api_version:
    raise Exception("❌ Missing OpenAI API credentials. Check .env file.")

# Determine azure_endpoint
azure_endpoint = os.getenv("AZURE_ENDPOINT")
if not azure_endpoint:
    resource_name = os.getenv("AZURE_RESOURCE_NAME")
    if resource_name:
        azure_endpoint = f"https://{resource_name}.openai.azure.com/"
    else:
        raise Exception("❌ Missing AZURE_ENDPOINT or AZURE_RESOURCE_NAME environment variable.")

# Initialize Azure OpenAI
print("🔹 Initializing Azure OpenAI Model...")
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
    print(f"❌ Failed to Initialize Azure OpenAI Model: {e}")
    raise

# Retrieve Snowflake credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def create_connection():
    """Create a Snowflake connection."""
    print("🔹 Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("✅ Snowflake connection successful!")
        return conn
    except Exception as e:
        print(f"❌ Snowflake Connection Failed: {e}")
        raise

def get_snowflake_metadata(conn):
    """Fetch Snowflake metadata."""
    print("🔹 Fetching Snowflake metadata...")
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
            raise ValueError("⚠️ No metadata retrieved! Check permissions.")

        metadata_df = pd.DataFrame(metadata_rows, columns=["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"])
        metadata_dict = (
            metadata_df.drop(columns=["TABLE_NAME"])
            .groupby(metadata_df["TABLE_NAME"], group_keys=False)
            .apply(lambda x: {col: dtype for col, dtype in zip(x["COLUMN_NAME"], x["DATA_TYPE"])} )
            .to_dict()
        )
        print("✅ Metadata retrieved successfully!")
        return metadata_dict
    except Exception as e:
        print(f"❌ Error fetching metadata: {str(e)}")
        return None

def query_snowflake(conn, sql_query):
    """Execute a SQL query in Snowflake."""
    print(f"🔹 Executing SQL Query:\n{sql_query}")
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()

        if not result:
            print("⚠️ Query returned no data!")
            return pd.DataFrame()

        print("✅ SQL Query executed successfully!")
        return pd.DataFrame(result, columns=columns)
    except Exception as e:
        print(f"❌ SQL Execution Error: {e}")
        return pd.DataFrame({"Error": [str(e)]})

def visual_generate(query, data, response):
    """Generate and encode a graph from the query results."""
    print("🔹 Generating visualization...")

    try:
        # Convert data to DataFrame
        df = pd.DataFrame(data)
        if df.empty or len(df.columns) < 2:
            print("⚠️ No data available to generate a graph.")
            return ""

        # Example: Generate a bar chart (modify as needed)
        fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=response)

        # Set theme
        fig.update_layout(
            plot_bgcolor="#2B2C2E",
            paper_bgcolor="#2B2C2E",
            font=dict(color="#FFFFFF"),
        )

        # Save the plot using Kaleido
        image_path = "graph.png"
        pio.write_image(fig, image_path, format="png", engine="kaleido")

        # Encode the image
        with open(image_path, 'rb') as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode("utf-8")

        print("✅ Visualization generated successfully!")
        return encoded_image

    except Exception as e:
        print(f"❌ Graph generation error: {e}")
        return ""
