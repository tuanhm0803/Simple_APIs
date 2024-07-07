from flask import Flask, jsonify, request
import psycopg2

# Replace with your database connection details
DATABASE_HOST = "localhost"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "250717"
DATABASE_NAME = "postgres"
DB_PORT = 5432

app = Flask(__name__)

@app.route("/data", methods=["GET"])
def get_data():
    # Connect to the database
    try:
        conn = psycopg2.connect(
            host=DATABASE_HOST,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD,
            database=DATABASE_NAME,
            port = DB_PORT
        )
    except psycopg2.Error as err:
        print(f"Error connecting to database: {err}")
        return jsonify({"error": "Database connection failed!"}), 500

    # Prepare the SQL query (replace with your specific query)
    query = "select * from stg.crypto_cur_info limit 1"

    # Create a cursor object
    cur = conn.cursor()

    # Execute the query
    cur.execute(query)
    rows = cur.fetchall()

    # Get column names
    colnames = [desc[0] for desc in cur.description]

    # Convert to list of dictionaries
    data = [dict(zip(colnames, row)) for row in rows]
    # Fetch results and convert to list of dictionaries
    #data = [dict(row) for row in cur.fetchall()]

    # Close the cursor and database connection
    cur.close()
    conn.close()

    # Return the data as JSON
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)
