#!/usr/bin/env python3
# main.py - Flask app with MySQL and Pub/Sub
from flask import Flask, render_template, request, jsonify
import os
import mysql.connector
from mysql.connector import errorcode
from google.cloud import pubsub_v1
from datetime import datetime
import json
import time

app = Flask(__name__, template_folder='templates')

# --- Configuration ---
DB_HOST = os.environ.get('DB_HOST', '10.15.208.2')
DB_USER = os.environ.get('DB_USER', 'user-asia')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'Password@123')
DB_NAME = os.environ.get('DB_NAME', 'db-asia')
DB_PORT = int(os.environ.get('DB_PORT', 3306))

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'app-log-477713')
PUBSUB_TOPIC = os.environ.get('PUBSUB_TOPIC', 'user-notification')
PUBSUB_FULL_TOPIC = f'projects/{PROJECT_ID}/topics/{PUBSUB_TOPIC}'

ALLOWED_USERS = [
    'pravinrajagcp@gmail.com',
    'pravinrajh1@example.com',
    'parthibank72@gmail.com'
]

# --- Database Connection ---
def get_db_connection(retries=3, delay=2):
    """Create MySQL connection with retry logic"""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                port=DB_PORT,
                connection_timeout=10,
                autocommit=False
            )
            return conn
        except mysql.connector.Error as exc:
            last_exc = exc
            app.logger.warning(f"DB connect attempt {attempt} failed: {exc}")
            time.sleep(delay)
    raise last_exc

# --- Database Operations ---
def log_activity_db(user_email, activity_type):
    """Insert activity record"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        insert_sql = ("INSERT INTO user_activity "
                      "(user_email, activity_type, timestamp) "
                      "VALUES (%s, %s, %s)")
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(insert_sql, (user_email, activity_type, now))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as e:
        app.logger.error(f"Error inserting activity: {e}")
        return False

def get_user_logs_db(user_email, limit=10):
    """Fetch recent user logs"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = ("SELECT id, user_email, activity_type, timestamp "
                 "FROM user_activity "
                 "WHERE user_email = %s "
                 "ORDER BY timestamp DESC "
                 "LIMIT %s")
        cursor.execute(query, (user_email, limit))
        rows = cursor.fetchall()
        for r in rows:
            if isinstance(r.get('timestamp'), datetime):
                r['timestamp'] = r['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        cursor.close()
        conn.close()
        return rows
    except mysql.connector.Error as e:
        app.logger.error(f"Error fetching logs: {e}")
        return []

# --- Pub/Sub Publishing ---
def publish_notification_pubsub(user_email):
    """Publish notification to Pub/Sub"""
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = PUBSUB_FULL_TOPIC
        message_data = {
            'user_email': user_email,
            'timestamp': datetime.utcnow().isoformat(),
            'message': f'Notification requested by {user_email}'
        }
        data = json.dumps(message_data).encode('utf-8')
        future = publisher.publish(topic_path, data)
        message_id = future.result(timeout=10)
        app.logger.info(f"Published message id: {message_id}")
        return True
    except Exception as e:
        app.logger.error(f"Pub/Sub publish error: {e}")
        return False

# --- IAP Helper ---
def get_user_email_from_iap():
    """Extract user email from IAP headers"""
    iap_email = request.headers.get('X-Goog-Authenticated-User-Email', '')
    if iap_email:
        parts = iap_email.split(':')
        if len(parts) > 1:
            return parts
    # Fallback for local testing
    return request.headers.get('X-Test-Email', 'pravinrajagcp@gmail.com')

def get_user_name_from_email(email):
    username = email.split('@')
    return username.replace('.', ' ').replace('_', ' ').title()

# --- Routes ---
@app.route('/')
def index():
    user_email = get_user_email_from_iap()
    if user_email not in ALLOWED_USERS:
        user_name = get_user_name_from_email(user_email)
        return render_template('unauthorized.html', 
                             user_name=user_name, 
                             user_email=user_email), 403
    
    log_activity_db(user_email, 'Page Load')
    logs = get_user_logs_db(user_email, limit=10)
    return render_template('dashboard.html', 
                         user_email=user_email, 
                         logs=logs)

@app.route('/api/logs')
def api_logs():
    user_email = get_user_email_from_iap()
    if user_email not in ALLOWED_USERS:
        return jsonify({'error': 'Unauthorized'}), 403
    logs = get_user_logs_db(user_email, limit=50)
    return jsonify({'logs': logs})

@app.route('/api/notify', methods=['POST'])
def api_notify():
    user_email = get_user_email_from_iap()
    if user_email not in ALLOWED_USERS:
        return jsonify({'error': 'Unauthorized'}), 403
    
    ok = publish_notification_pubsub(user_email)
    if ok:
        log_activity_db(user_email, 'Notification Sent')
        return jsonify({'message': 'Notification published'}), 200
    else:
        return jsonify({'error': 'Failed to publish notification'}), 500

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
