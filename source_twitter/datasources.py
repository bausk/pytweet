import os
import json
import twitter
import got3 as twitter_scraper
import datetime

from typing import List

TWITTER_AUTH = json.loads(os.getenv("TWITTER_AUTH", "{}"))
api = twitter.Api(**TWITTER_AUTH)


def get_users(keyword, count=10) -> List[twitter.User]:
    page = 1
    results = []
    while True:
        try:
            results_page = api.GetUsersSearch(term=keyword, count=100 if count > 100 else count, page=page)
        except:
            results_page = []
        results.extend(results_page)
        if len(results) >= count or len(results_page) == 0:
            return results
        page += 1


def get_user_tweets(screen_name, method="api", count=100, since=None, until=None):
    max_id = None
    results = []
    if method == "scraper":
        criteria = twitter_scraper.manager.TweetCriteria()
        criteria.username = screen_name
        criteria.since = since
        criteria.until = until
        criteria.maxTweets = count
        return twitter_scraper.manager.TweetManager.getTweets(criteria)

    while True:
        results_page = api.GetUserTimeline(
            screen_name=screen_name,
            max_id=max_id,
            count=300 if count > 100 else count,
            trim_user=True
        )

        since_hit = False
        since_datetime = datetime.datetime.strptime(since, "%Y-%m-%d")
        if datetime.datetime.utcfromtimestamp(results_page[-1].created_at_in_seconds) < since_datetime:
            for i, e in enumerate(results_page):
                if datetime.datetime.utcfromtimestamp(e.created_at_in_seconds) < since_datetime:
                    del results_page[i:]
                    since_hit = True
                    break

        if len(results_page) == 0:
            return results

        results.extend(results_page)

        until_datetime = datetime.datetime.strptime(until, "%Y-%m-%d") + datetime.timedelta(days=1)
        if datetime.datetime.utcfromtimestamp(results_page[0].created_at_in_seconds) > until_datetime:
            for i, e in enumerate(results):
                if datetime.datetime.utcfromtimestamp(e.created_at_in_seconds) < until_datetime:
                    del results[:i]
                    break

        if len(results) >= count or since_hit:
            return results

        max_id = results[-1].id
