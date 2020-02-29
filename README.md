# Reddit processing

System for scrapping submissions from reddit service using celery. 

You can download model for embedding from magnistude and save to embedding_worker directory by command:
```
wget magnitude.plasticity.ai/glove/light/glove.twitter.27B.25d.magnitude -P embedding_worker
```

You need to provide credentials to your reddit application for PRAW scrapper as JSON in following format:

```
{
    "client_id": "",
    "client_secret": "",
    "username": "",
    "password": "",
    "user_agent": ""
}
```

In scrapper_worker/praw_credentials.json file.
