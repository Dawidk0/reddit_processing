"""Scheduler for scrapping."""
from celery import Celery
from shared.docker_logs import get_logger

logging = get_logger("scheduler")
app = Celery()
app.config_from_object('config')
