from flask import Flask, jsonify
from google.cloud import bigquery
import requests
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from twilio.rest import Client

app = Flask(__name__)
client = bigquery.Client()

# Twilio & SendGrid API Keys
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE = "+17788378660"
EMAIL_SENDER = "anilasatyavolu@gmail.com"

DATASET_ID = "user_data"
USER_TABLE = "user_input_table"
WEATHER_TABLE = "weather_data"
NOTIF_TABLE = "notifications"

# Service 1 API URL
SERVICE_1_URL = os.getenv("https://user-subscription-service-anila-235097227455.us-central1.run.app")  # Store Service 1 URL as an env variable

def fetch_subscribed_users():
    """Fetch users from Service 1 who have subscribed for notifications."""
    try:
        response = requests.get(f"{SERVICE_1_URL}/get_subscribed_users", timeout=5)
        if response.status_code == 200:
            return response.json().get("users", [])
        else:
            print(f"Error fetching users from Service 1: {response.text}")
            return []
    except Exception as e:
        print(f"Error connecting to Service 1: {str(e)}")
        return []

def fetch_weather(user_id, location):
    """Fetch latest weather data for the user."""
    query = f"""
    SELECT temperature, weather_description, humidity
    FROM `{DATASET_ID}.{WEATHER_TABLE}`
    WHERE user_id = @user_id AND location = @location
    ORDER BY timestamp DESC
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("location", "STRING", location)
        ]
    )
    result = list(client.query(query, job_config=job_config).result())
    return result[0] if result else None

def send_email(recipient, subject, message):
    """Send email via SendGrid."""
    mail = Mail(
        from_email=EMAIL_SENDER,
        to_emails=recipient,
        subject=subject,
        plain_text_content=message
    )
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(mail)
        return "Sent"
    except Exception as e:
        print(f"Email error: {str(e)}")
        return "Failed"

def send_sms(recipient, message):
    """Send SMS via Twilio."""
    try:
        client = Client(TWILIO_SID, TWILIO_AUTH_TOKEN)
        client.messages.create(
            body=message,
            from_=TWILIO_PHONE,
            to=recipient
        )
        return "Sent"
    except Exception as e:
        print(f"SMS error: {str(e)}")
        return "Failed"

def log_notification(user_id, location, method, status, message):
    """Log notification status in BigQuery."""
    rows_to_insert = [
        {
            "user_id": user_id,
            "location": location,
            "notification_method": method,
            "status": status,
            "message": message,
        }
    ]
    client.insert_rows_json(f"{DATASET_ID}.{NOTIF_TABLE}", rows_to_insert)

@app.route('/send_notifications', methods=['POST'])
def send_notifications():
    """Fetch users from Service 1, get weather, and send notifications."""
    users = fetch_subscribed_users()
    if not users:
        return jsonify({"message": "No subscribed users found."}), 200

    notifications = []

    for user in users:
        user_id = user["user_id"]
        location = user["location"]
        email_id = user.get("email_id")
        phone_number = user.get("phone_number")
        notification_method = user.get("notification_method", [])

        weather = fetch_weather(user_id, location)
        if not weather:
            continue

        message = (f"Hello! Weather update for {location}: "
                   f"{weather['weather_description']}, "
                   f"Temp: {weather['temperature']}°C, "
                   f"Humidity: {weather['humidity']}%.")

        for method in notification_method:
            if method == "email" and email_id:
                status = send_email(email_id, "Weather Update", message)
            elif method == "SMS" and phone_number:
                status = send_sms(phone_number, message)
            else:
                continue

            log_notification(user_id, location, method, status, message)
            notifications.append({"user_id": user_id, "method": method, "status": status})

    return jsonify({"notifications_sent": notifications}), 200

@app.route('/notification_logs', methods=['GET'])
def get_notification_logs():
    """Retrieve notification logs."""
    query = f"SELECT * FROM `{DATASET_ID}.{NOTIF_TABLE}` ORDER BY timestamp DESC"
    results = client.query(query).result()
    return jsonify([dict(row) for row in results])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
