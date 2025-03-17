import os
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.io as pio
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
import logging
import sys

# 🔹 Configure Logger
def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(name)s [%(asctime)s] [%(levelname)s] %(message)s'))
    logger.addHandler(handler)
    return logger

logger = get_logger('snowflake-service')

# 🔹 Load environment variables
logger.debug("Loading environment variables...")
load_dotenv()

# 🔹 Retrieve Azure OpenAI credentials
api_key = os.getenv("OPENAI_API_KEY")
deployment_name = os.getenv("AZURE_DEPLOYMENT_NAME")
api_version = os.getenv("AZURE_API_VERSION")
azure_endpoint = os.getenv("AZURE_ENDPOINT")

# 🔹 Initialize Azure OpenAI
logger.debug("Initializing Azure OpenAI Model...")
try:
    llm = AzureChatOpenAI(
        openai_api_key=api_key,
        azure_endpoint=azure_endpoint,
        deployment_name=deployment_name,
        api_version=api_version,
        temperature=0.0
    )
    logger.debug("✅ Azure OpenAI Model initialized successfully!")
except Exception as e:
    logger.error(f"🚨 Failed to Initialize Azure OpenAI Model: {e}")
    raise

# 🔹 Retrieve Snowflake credentials (Ensure no password for OAuth inside Snowflake)
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")  # Used for local testing
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")

def get_login_token():
    """
    Retrieve the short-lived Snowflake OAuth token if running inside Snowflake Container Services.
    If running locally, return None.
    """
    token_path = "/snowflake/session/token"
    if os.path.exists(token_path):
        try:
            with open(token_path, "r") as f:
                return f.read().strip()
        except Exception as e:
            logger.error(f"🚨 Error reading OAuth token: {e}")
            raise
    else:
        logger.warning("⚠️ Running locally: No OAuth token file found. Falling back to password authentication.")
        return None

def get_connection_params():
    """
    Dynamically selects authentication method:
    - OAuth authentication when running inside Snowflake Container Services.
    - Username/password authentication when running locally.
    """
    token = get_login_token()

    if token:
        logger.debug("✅ Using OAuth authentication (inside Snowflake Container Services).")
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "authenticator": "oauth",
            "token": token,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }
    else:
        logger.debug("✅ Using username/password authentication (running locally).")
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "user": SNOWFLAKE_USER,
            "password": SNOWFLAKE_PASSWORD,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }

def create_connection():
    """Establish a Snowflake connection using the appropriate authentication method."""
    try:
        conn = snowflake.connector.connect(**get_connection_params())
        logger.debug("✅ Snowflake connection established successfully!")
        return conn
    except Exception as e:
        logger.error(f"🚨 Snowflake Connection Failed: {e}")
        raise

def get_snowflake_metadata(conn):
    """Fetch metadata from Snowflake."""
    logger.debug("Fetching Snowflake metadata...")
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
            logger.warning("⚠️ No metadata retrieved! Check Snowflake permissions.")
            return {}

        metadata_df = pd.DataFrame(metadata_rows, columns=["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"])
        metadata_dict = (
            metadata_df.drop(columns=["TABLE_NAME"])
            .groupby(metadata_df["TABLE_NAME"], group_keys=False)
            .apply(lambda x: {col: dtype for col, dtype in zip(x["COLUMN_NAME"], x["DATA_TYPE"])}).
            to_dict()
        )
        logger.debug("✅ Metadata retrieved successfully!")
        return metadata_dict
    except Exception as e:
        logger.error(f"🚨 Error fetching metadata: {str(e)}")
        return None

def query_snowflake(conn, sql_query):
    """Execute a SQL query in Snowflake."""
    logger.debug(f"Executing SQL query: {sql_query}")
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()

        if not result:
            logger.warning("⚠️ Query returned no data!")
            return pd.DataFrame()

        logger.debug("✅ SQL query executed successfully!")
        return pd.DataFrame(result, columns=columns)
    except Exception as e:
        logger.error(f"🚨 SQL Execution Error: {e}")
        return pd.DataFrame({"Error": [str(e)]})

def visual_generate(query, data, response):
    """
    Generate an interactive HTML chart from the query results.
    Returns an HTML string or an empty string if generation fails.
    """
    logger.debug("Attempting to generate interactive HTML visualization...")
    try:
        df = pd.DataFrame(data)
        if df.empty or len(df.columns) < 2:
            logger.warning("⚠️ Not enough data to generate a chart.")
            return ""
        
        fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=response)
        fig.update_layout(
            plot_bgcolor="#2B2C2E",
            paper_bgcolor="#2B2C2E",
            font=dict(color="#FFFFFF"),
        )
        html_str = pio.to_html(fig, full_html=False)
        logger.debug("✅ HTML visualization generated successfully!")
        return html_str
    except Exception as e:
        logger.error(f"🚨 HTML chart generation error: {e}")
        return ""

__all__ = ["create_connection", "get_snowflake_metadata", "query_snowflake", "visual_generate", "llm"]
