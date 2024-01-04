# app.py
from flask import Flask, request, jsonify
# from rq import Queue
# from worker import conn
from redis import Redis
import pandas as pd
from tasks import queue_click
from celery import Celery

app = Flask(__name__)

# Define the path to the CSV files
csv_file_path = 'user_clicks.csv'

# Create an empty DataFrame or load existing data if the file exists
try:
    df = pd.read_csv(csv_file_path)
except FileNotFoundError:
    df = pd.DataFrame(columns=['user_id', 'timestamp', 'cctv_location'])


# Create a Celery app (ONLY SUPPORT UNTIL CELERY 3 on WINDOWS)
#app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'  # Update with your Redis configuration
#app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'  # Update with your Redis configuration
#celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
#celery.conf.update(app.config)
#celery_instance = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
#celery_instance.conf.update(app.config)
# Create a Redis Queue
# redis_address = '127.0.1.1'
# redis_port = 6379

# conn = Redis(host=redis_address, port=redis_port)
# q = Queue(connection=conn)

@app.route('/record_click', methods=['POST'])
def record_click():
    try:
        # Extract data from the request body
        data = request.get_json()

        # Enqueue the task to process the click asynchronously
        # q.enqueue(process_click, data)

        #celery
        queue_click.delay(data) 

        return jsonify({'message': 'Click recorded successfully. Processing asynchronously.'}), 202
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
