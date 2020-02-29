"""Import datatime module for scheduling."""
from datetime import timedelta

CELERYBEAT_SCHEDULE = {
    'get_reddits': {
        'task': 'get_reddits',
        'schedule': timedelta(seconds=30),
        'options': {'queue': 'scr',
                    'routing_key': 'scr'}
    },

}
