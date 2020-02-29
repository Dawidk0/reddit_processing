"""Necessary imports for embedding worker."""
from celery import Celery
from pymagnitude import Magnitude
from shared.docker_logs import get_logger
from kombu import Queue
worker_name = 'emb'
logging = get_logger(worker_name)
app = Celery()

vectors = Magnitude('./glove.twitter.27B.25d.magnitude')
app.conf.task_serializer = 'pickle'
app.conf.result_serializer = 'pickle'
app.conf.accept_content = ['json', 'pickle']

queue = Queue(
    worker_name,
    'celery',
    routing_key=worker_name)
app.conf.task_queues = [queue]

app.conf.task_default_queue = worker_name


@app.task(bind=True, name='emb_reddits')
def emb_reddits(task, submissions):
    """Task to add word embeddings to data model."""
    logging.info(worker_name)
    for sub in submissions:
        sub.post_text_emb = vectors.query(sub.post_text.split(" ")).tolist()
    return submissions
