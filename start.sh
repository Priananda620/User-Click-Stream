celery -A tasks worker --loglevel=info -c 1

python3 producer.py

