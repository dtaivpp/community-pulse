from os import getenv
from datetime import datetime
import time
import logging
import tweepy
from community_pulse.util import to_ndjson, to_opensearch, backoff, get_os_client, matches_filter, jsonpath_filters

logger = logging.getLogger('community-pulse')


def get_data(querystring, translate: bool, ignore: list):
  """
  Get Tweets is a consumable stream of tweets that match the arg params
  """
  from html import unescape
  if translate:
    from community_pulse.util import translate_text

  ignore_filters = jsonpath_filters(ignore)

  client = create_twitter_client()
  # Needs updating such that if the marker wasn't from within the last
  # 7 days it returns none
  most_recent_tweet_id = get_marker()

  tw_detail = []
  tw_index = f"tweets-{datetime.date(datetime.now())}"

  tweets = tweepy.Paginator(client.search_recent_tweets,
                            querystring,
                            tweet_fields=['id',
                                          'author_id',
                                          'text',
                                          'lang',
                                          'public_metrics',
                                          'created_at',
                                          'entities'],
                            expansions=['author_id', 'referenced_tweets.id'],
                            user_fields=['username'],
                            max_results=50,
                            since_id=most_recent_tweet_id)
  # reverse=True)

  for tweet_page in tweets_iterator(tweets):
    # for tweet_page in tweets:
    # If no tweets break
    if tweet_page.data is None:
      break

    users = {u["id"]: u for u in tweet_page.includes['users']}

    if 'tweets' in tweet_page.includes:
      referenced_tweets = {
          t["id"]: t.text for t in tweet_page.includes['tweets']}

    for tweet in tweet_page.data:
      # need to store referenced tweet data on the tweet itself.

      mode = {"index": {"_index": tw_index, "_id": tweet.id}}
      tmp_tweet = dict(tweet)

      # Used to restore full text if the referenced tweet is a reply/reweet
      if 'referenced_tweets' in tweet:
        for ref_tweet in tweet.referenced_tweets:
          if ref_tweet.type == "retweeted":
            tmp_tweet['text'] = referenced_tweets[ref_tweet.id]

          if ref_tweet.type != "replied_to":
            tmp_tweet['referenced'] = referenced_tweets[ref_tweet.id]

      # Translate if the tweet isn't in English
      if translate and tweet.lang not in ['en', '', 'zxx', 'qme', 'qht', 'und']:
        tmp_tweet['text'] = translate_text(tmp_tweet['text'])

      tmp_tweet['text'] = unescape(tmp_tweet['text'])

      tmp_tweet['full_lang'] = full_lang(tweet.lang)
      tmp_tweet['username'] = users[tweet.author_id].username
      tmp_tweet['url'] = f"https://twitter.com/{users[tweet.author_id].username}/status/{tweet.id}"  # pylint: disable=[C0301]
      tmp_tweet['false_match'] = False

      if not matches_filter(ignore_filters, tmp_tweet):
        tw_detail.append(mode)
        tw_detail.append(tmp_tweet)

    logger.debug("Total Collected Tweets: %s", len(tw_detail)/2)
  return tw_detail


def tweets_iterator(iterator):
  """Uses an iterator on the pagination to apply backoff to failed requests"""
  twpages = iter(iterator)
  sleep_time = 4
  while twpages:
    time.sleep(sleep_time)
    try:
      data = next(twpages)
      yield data
      sleep_time = 4
    except (tweepy.errors.TwitterServerError, tweepy.errors.TooManyRequests):
      logger.debug("Twitter 503/429. Backing off for: %ss", sleep_time)
      sleep_time = backoff(sleep_time)
    except StopIteration:
      return None


def create_twitter_client() -> tweepy.Client:
  """Returns twitter client"""
  try:
    token = getenv("TW_BEARER_TOKEN")
    client = tweepy.Client(bearer_token=token)
  except Exception as err:
    logger.exception(err)
  return client


def gen_twitter_executor():
  """Wraps the twitter execuition pipeline"""
  def execute_twitter(query: str, translate=False, ignore={}):
    tweets = get_data(querystring=query,
                      translate=translate,
                      ignore=ignore)

    to_opensearch(to_ndjson(tweets))
  return execute_twitter


def full_lang(tweet_lang: str) -> str:
  """Transates the ISO639 Language Code into full name"""
  from iso639 import languages

  lang_dict = {
      'und': 'Undefined',
      'zxx': 'No linguistic content',
      'qme': 'Media Links',
      'qht': 'Hashtags',
      'in': 'Indonesian'
  }

  lang = ""
  try:
    if tweet_lang in lang_dict:
      lang = lang_dict.get(tweet_lang)
    elif tweet_lang != '':
      lang = languages.get(part1=tweet_lang).name

  except:
    logger.error("Language not found: %s", tweet_lang)

  return lang


def get_marker():
  """Get the last indexed tweet ID"""
  os_client = get_os_client()
  query = {
      "query": {
        "bool": {
          "filter": [
            {
              "range": {
                "created_at": {
                  "gte": "now-7d/d"
                }
              }
            }
          ]
        }
      },
      "fields": ["_id"],
      "sort": [
        {"created_at": {"order": "desc"}},
        {"_score": {"order": "desc"}}
      ],
      "size": 1
  }
  result = os_client.search(index='tweets*', body=query)

  try:
    _id = result['hits']['hits'][0]['_id']
    return _id
  except:
    return None
