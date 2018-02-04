
import os
import json
import twitter

TWITTER_AUTH = json.loads(os.getenv("TWITTER_AUTH", "{}"))

api = twitter.Api(**TWITTER_AUTH)
since_id = None
sum = 0

while True:
    config = dict(term="blockchain", result_type="recent", since="2018-01-20", until="2018-01-21", count=100)
    if since_id:
        config["max_id"] = since_id
    results = api.GetSearch(**config)
    sum += len(results)
    print("{}, ({})".format(len(results), sum))
    print(results[-1].created_at)

    if results[-1].id == since_id:
        break
    since_id = results[-1].id
