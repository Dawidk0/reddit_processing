"""Necessary imports for scrapping worker."""
from shared.model.reddit_post import RedditPost
from datetime import datetime
import json
import praw
from celery import Celery, chain
from shared.docker_logs import get_logger
from kombu import Queue

worker_name = 'scr'
logging = get_logger(worker_name)
app = Celery()

queue = Queue(
    worker_name,
    'celery',
    routing_key=worker_name)
app.conf.task_queues = [queue]
app.conf.task_serializer = 'pickle'
app.conf.result_serializer = 'pickle'
app.conf.task_default_queue = worker_name

with open('praw_credentials.json') as json_file:
    credentials = json.load(json_file)

reddit_client = praw.Reddit(
    client_id=credentials['client_id'],
    client_secret=credentials['client_secret'],
    username=credentials['username'],
    password=credentials['password'],
    user_agent=credentials['user_agent'])


@app.task(bind=True, name='get_reddits')
def get_reddits(task):
    """Task for scrapping reddits."""
    logging.info(worker_name)

    with open("timestamp.txt", "r") as time_file:
        last_submission_time = datetime.fromtimestamp(float(time_file.read()))

    subreddit = reddit_client.subreddit('all')
    top_subreddits = subreddit.new(limit=100)

    submissions = []

    for i, subm in enumerate(top_subreddits):
        if datetime.fromtimestamp(subm.created_utc) > last_submission_time:
            if i == 0:
                with open("timestamp.txt", "w") as time_file:
                    time_file.write(str(subm.created_utc))
            submissions.append(
                RedditPost(subm.url, subm.author.name,
                           subm.subreddit.display_name,
                           subm.title, [[]], subm.ups + subm.downs,
                           subm.subreddit.display_name == 'nsfw',
                           subm.num_comments))

    if len(submissions) > 0:
        chain(
            app.signature('emb_reddits', queue='emb', routing_key='emb',
                          args=[submissions]),
            app.signature('save_reddits', queue='db', routing_key='db')
        ).apply_async()
