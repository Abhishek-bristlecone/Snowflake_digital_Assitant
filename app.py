import os
import re
import pandas as pd
from flask import Flask, request, jsonify

# Import the utility functions and the LLM from utils.py
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
    data = request.get_json()
    user_question = data.get("user_question")
    if not user_question:
        return jsonify({"message": "No user_question provided", "result": {}}), 400

    # Create Snowflake connection
    conn = create_connection()

    # Retrieve metadata
    snowflake_metadata = get_snowflake_metadata(conn)
    if not snowflake_metadata:
        conn.close()
        return jsonify({"message": "Metadata retrieval failed.", "result": {}}), 500

    # Read system instructions (prompt) from file
    with open("instructions.txt", "r", encoding="utf-8") as file:
        system_prompt = file.read().strip()

    # Combine instructions + user question
    metadata_prompt = f"{system_prompt}\n\nUser Question:\n{user_question}"

    # Invoke LLM to generate SQL
    try:
        llm_response = llm.invoke(metadata_prompt).content.strip()
        sql_match = re.search(r"```sql\n(.*?)\n```", llm_response, re.DOTALL)
        if sql_match:
            sql_query = sql_match.group(1).strip()
        else:
            conn.close()
            return jsonify({"message": "LLM did not return a valid SQL query format.", "result": {}}), 500
    except Exception as e:
        conn.close()
        return jsonify({"message": f"Error generating SQL query: {str(e)}", "result": {}}), 500

    # Execute SQL in Snowflake
    result_df = query_snowflake(conn, sql_query)

    # Handle case where query returns no results
    if result_df.empty:
        conn.close()
        return jsonify({"message": "Query returned no data.", "result": [], "image": ""}), 200

    # Ask LLM to explain the results in one sentence
    explanation_prompt = f"Explain the meaning of the following query results in one sentence:\n{result_df.head(10).to_json()}"
    explanation_response = llm.invoke(explanation_prompt).content.strip()

    # Close the Snowflake connection
    conn.close()

    # Convert results to a list of dictionaries
    result_list = result_df.to_dict(orient="records")

    # Generate a Plotly-based visualization (if possible)
    graph_png = visual_generate(sql_query, result_list, explanation_response)

    # If graph generation fails, return a message
    if not graph_png:
        graph_png_url = ""
    else:
        graph_png_url = f"data:image/png;base64,{graph_png}"

    return jsonify({
        "message": explanation_response,
        "result": result_list,
        "image": graph_png_url
    })

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
