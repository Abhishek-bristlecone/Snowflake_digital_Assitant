import os
import re
import pandas as pd
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Import from utils.py
from utils import (
    create_connection,
    get_snowflake_metadata,
    query_snowflake,
    visual_generate,
    llm
)


app = Flask(__name__)

@app.route('/getdata', methods=['POST'])
def query_api():
    print("Received POST request on /getdata")

    data = request.get_json()
    user_question = data.get("user_question")
    if not user_question:
        print(" No user_question provided.")
        return jsonify({"message": "No user_question provided", "result": {}}), 400

    # 1. Create Snowflake connection
    conn = create_connection()

    # 2. Retrieve metadata
    snowflake_metadata = get_snowflake_metadata(conn)
    if not snowflake_metadata:
        print(" Metadata retrieval failed.")
        conn.close()
        return jsonify({"message": "Metadata retrieval failed.", "result": {}}), 500

    # 3. Read system instructions (prompt) from file
    print("Reading system prompt from instructions.txt...")
    with open("instructions.txt", "r", encoding="utf-8") as file:
        system_prompt = file.read().strip()

    # 4. Combine instructions + user question
    metadata_prompt = f"{system_prompt}\n\nUser Question:\n{user_question}"
    print("Generating SQL via LLM...")
    try:
        llm_response = llm.invoke(metadata_prompt).content.strip()
        sql_match = re.search(r"```sql\n(.*?)\n```", llm_response, re.DOTALL)
        if sql_match:
            sql_query = sql_match.group(1).strip()
            print(f" LLM-generated SQL:\n{sql_query}")
        else:
            print(" LLM did not return a valid SQL query format.")
            conn.close()
            return jsonify({"message": "LLM did not return a valid SQL query format.", "result": {}}), 500
    except Exception as e:
        print(f" Error generating SQL query: {e}")
        conn.close()
        return jsonify({"message": f"Error generating SQL query: {str(e)}", "result": {}}), 500

    # 5. Execute SQL in Snowflake
    result_df = query_snowflake(conn, sql_query)

    # 6. Handle case where query returns no results
    if result_df.empty:
        print(" Query returned no data.")
        conn.close()
        return jsonify({
            "message": "Query returned no data.",
            "result": [],
            "chart_html": ""
        }), 200

    # 7. Ask LLM to explain the results in one sentence
    explanation_prompt = f"Explain the meaning of the following query results in one sentence:\n{result_df.head(10).to_json()}"
    print(" Generating explanation from LLM...")
    explanation_response = llm.invoke(explanation_prompt).content.strip()
    print(f"Explanation: {explanation_response}")

    # Close Snowflake connection
    conn.close()

    # Convert results to a list of dictionaries
    result_list = result_df.to_dict(orient="records")

    # 8. Generate an interactive HTML chart from query results
    print("Attempting to generate an interactive HTML chart from query results...")
    chart_html = visual_generate(sql_query, result_list, explanation_response)
    if not chart_html:
        print("No chart generated or chart generation failed.")
        chart_html = ""

    print("Returning final response with results and HTML chart (if any).")
    return jsonify({
        "message": explanation_response,
        "result": result_list,
        "chart_html": chart_html
    })

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
