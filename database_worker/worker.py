"""Necessary imports for database worker."""
from celery import Celery
from shared.docker_logs import get_logger
import pymongo
from kombu import Queue

worker_name = 'db'
logging = get_logger(worker_name)
app = Celery()

queue = Queue(
    worker_name,
    'celery',
    routing_key=worker_name)
app.conf.task_queues = [queue]

app.conf.task_serializer = 'pickle'
app.conf.result_serializer = 'pickle'
app.conf.accept_content = ['json', 'pickle']
app.conf.task_default_queue = worker_name

myclient = pymongo.MongoClient('mongodb://mongodb:27017/')
mydb = myclient["lsdp"]
mycol = mydb["reddits"]


@app.task(bind=True, name='save_reddits')
def save_reddits(task, submissions):
    """Task for saving reddits in database."""
    logging.info(worker_name)
    mycol.insert_many([sub.to_dict() for sub in submissions])
