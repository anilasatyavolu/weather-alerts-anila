from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)
client = bigquery.Client()

# BigQuery dataset and table details
DATASET_ID = 'user_data' 
TABLE_ID = 'user_input_table'

@app.route('/subscribe', methods=['POST'])
def subscribe():
    """
    Registers a new user with their preferences, ensuring:
    - `user_id` is unique.
    - Either `email_id` or `phone_number` must be provided (at least one).
    - `notification_method` must be either 'email' or 'SMS' (or both).
    - `preferred_units` must be 'Celsius' or 'Fahrenheit'.
    """
    data = request.json

    # Ensure required fields are present
    required_fields = ["user_id", "location", "notification_method"]
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    user_id = data["user_id"]

    # Check if the user_id already exists
    if user_exists(user_id):
        return jsonify({"error": "User ID already exists. Please choose a different one."}), 400

    # Ensure at least one of email_id or phone_number is provided
    email_id = data.get("email_id")
    phone_number = data.get("phone_number")
    
    if not email_id and not phone_number:
        return jsonify({"error": "Either email_id or phone number is required."}), 400

    # Validate notification method
    notification_method = data["notification_method"]
    valid_methods = ["email", "SMS"]
    if not isinstance(notification_method, list) or not all(method in valid_methods for method in notification_method):
        return jsonify({"error": "Invalid notification method. Choose 'email' or 'SMS'."}), 400

    # Validate preferred temperature units
    preferred_units = data.get("preferred_units", "Celsius")  # Default to Celsius
    if preferred_units not in ["Celsius", "Fahrenheit"]:
        return jsonify({"error": "Invalid preferred_units. Choose 'Celsius' or 'Fahrenheit'."}), 400

    # Save user to BigQuery
    save_user_to_bigquery(user_id, email_id, phone_number, data["location"], notification_method, preferred_units)

    return jsonify({"message": "User subscribed successfully!"}), 201

@app.route('/users', methods=['GET'])
def get_users():
    """
    Returns all subscribed users from BigQuery.
    """
    users_data = get_users_from_bigquery()
    return jsonify(users_data)

def user_exists(user_id):
    query = f"""
    SELECT COUNT(*) as user_count
    FROM `{DATASET_ID}.{TABLE_ID}`
    WHERE user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()
    return list(result)[0].user_count > 0

def save_user_to_bigquery(user_id, email_id, phone_number, location, notification_method, preferred_units):
    rows_to_insert = [
        {
            "user_id": user_id,
            "email_id": email_id,
            "phone_number": phone_number,
            "location": location,
            "notification_method": notification_method,
            "preferred_units": preferred_units,
        }
    ]
    errors = client.insert_rows_json(f"{DATASET_ID}.{TABLE_ID}", rows_to_insert)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")

def get_users_from_bigquery():
    query = f"SELECT * FROM `{DATASET_ID}.{TABLE_ID}`"
    query_job = client.query(query)
    results = query_job.result()
    return [dict(row) for row in results]

if __name__ == '__main__':
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)