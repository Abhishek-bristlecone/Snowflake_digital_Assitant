import os
import re
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS  # Enable CORS for external requests
from dotenv import load_dotenv
import logging
import sys

# Configure logging
def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter('%(name)s [%(asctime)s] [%(levelname)s] %(message)s'))
    logger.addHandler(handler)
    return logger

logger = get_logger('Flask-App')

# Load environment variables
load_dotenv()

# Import functions from utils.py
from utils import (
    create_connection,
    get_snowflake_metadata,
    query_snowflake,
    visual_generate,
    llm
)

# Initialize Flask App
app = Flask(__name__)

# Enable CORS for all domains (Required for Snowflake)
CORS(app)

# ✅ Health Check Endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "running"}), 200

# ✅ API to Query Snowflake (Supports both GET and POST)
@app.route('/getdata', methods=['GET', 'POST'])
def query_api():
    logger.debug("Received request on /getdata")

    # Determine if the request is GET or POST
    if request.method == 'POST':
        data = request.get_json()
        user_question = data.get("data") if data else None
    elif request.method == 'GET':
        user_question = request.args.get("data")  # Get query param ?data=

    if not user_question:
        logger.warning("⚠️ No query provided in request")
        return jsonify({"message": "No user_question provided", "result": {}}), 400

    # 1. Create Snowflake connection
    conn = create_connection()

    # 2. Retrieve metadata
    snowflake_metadata = get_snowflake_metadata(conn)
    if not snowflake_metadata:
        conn.close()
        return jsonify({"message": "Metadata retrieval failed.", "result": {}}), 500

    # 3. Read system instructions (prompt) from file
    try:
        with open("instructions.txt", "r", encoding="utf-8") as file:
            system_prompt = file.read().strip()
    except FileNotFoundError:
        return jsonify({"message": "Instructions file not found"}), 500

    # 4. Generate SQL using LLM
    metadata_prompt = f"{system_prompt}\n\nUser Question:\n{user_question}"
    try:
        llm_response = llm.invoke(metadata_prompt).content.strip()
        sql_match = re.search(r"```sql\n(.*?)\n```", llm_response, re.DOTALL)
        if sql_match:
            sql_query = sql_match.group(1).strip()
        else:
            conn.close()
            return jsonify({"message": "Invalid SQL generated.", "result": {}}), 500
    except Exception as e:
        conn.close()
        return jsonify({"message": f"Error generating SQL: {str(e)}", "result": {}}), 500

    # 5. Execute SQL in Snowflake
    result_df = query_snowflake(conn, sql_query)
    conn.close()

    if result_df.empty:
        return jsonify({
            "message": "Query returned no data.",
            "result": [],
            "chart_html": ""
        }), 200

    # 6. Generate an explanation from LLM
    explanation_prompt = f"Explain these results: {result_df.head(10).to_json()}"
    explanation_response = llm.invoke(explanation_prompt).content.strip()

    # 7. Generate visualization
    chart_html = visual_generate(sql_query, result_df.to_dict(orient="records"), explanation_response)

    return jsonify({
        "message": explanation_response,
        "result": result_df.to_dict(orient="records"),
        "chart_html": chart_html
    })

# Set dynamic port from environment or default to 8080
port = int(os.getenv("SERVER_PORT", 8080))

# ✅ Ensure Flask binds to all interfaces
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=port, debug=True)
