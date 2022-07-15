from os import getenv
from opensearchpy import OpenSearch
from util import to_ndjson, to_opensearch
import tweepy


def get_data(querystring, os_client, translate: bool, ignore):
    """
    Get Tweets is a consumable stream of tweets that match the arg params
    """
    from datetime import datetime, timedelta
    from util import set_marker
    from html import unescape
    if translate:
        from util import translate_text

    cl = create_twitter_client()
    most_recent_tweet_id = get_marker(os_client)

    tw_detail = []
    tw_index = f"tweets-{datetime.date(datetime.now())}"

    tweets = tweepy.Paginator(cl.search_recent_tweets, 
                                querystring,
                                tweet_fields=['id', 'author_id', 'text', 'lang', 'public_metrics', 'created_at', 'entities'],
                                expansions=['author_id', 'referenced_tweets.id'], 
                                user_fields=['username'],
                                since_id=most_recent_tweet_id)

    for tweet_page in tweets:
        # If no tweets break
        if tweet_page.data == None:
            break

        users = {u["id"]: u for u in tweet_page.includes['users']}

        if 'tweets' in tweet_page.includes:
            referenced_tweets = {t["id"]: t.text for t in tweet_page.includes['tweets']}

        for tweet in tweet_page.data:
            # need to store referenced tweet data on the tweet itself.

            mode = { "index": { "_index": tw_index, "_id": tweet.id } }
            tmp_tweet = dict(tweet)

            # Used to restore full text if the referenced tweet is a reply/reweet
            if 'referenced_tweets' in tweet:
              for ref_tweet in tweet.referenced_tweets:
                if ref_tweet.type == "retweeted":
                  tmp_tweet['text'] = referenced_tweets[ref_tweet.id]

                if ref_tweet.type != "replied_to":
                  tmp_tweet['referenced'] = referenced_tweets[ref_tweet.id]

            # Translate if the tweet isn't in English
            if translate and tweet.lang not in ['en', '', 'zxx', 'qme', 'qht', 'und' ]:
                tmp_tweet['text'] = translate_text(tmp_tweet['text'])
            
            tmp_tweet['text'] = unescape(tmp_tweet['text'])

            tmp_tweet['full_lang'] = full_lang(tweet.lang)
            tmp_tweet['username'] = users[tweet.author_id].username
            tmp_tweet['url'] = f"https://twitter.com/{users[tweet.author_id].username}/status/{tweet.id}"
            tmp_tweet['false_match'] = False

            tw_detail.append(mode)
            tw_detail.append(tmp_tweet)

    return tw_detail


def create_twitter_client() -> tweepy.Client:
    """Returns twitter client"""
    try:
        token = getenv("TW_BEARER_TOKEN")
        cl = tweepy.Client(bearer_token=token)
    except Exception as e:
        print(e)
    return cl


def gen_twitter_executor(os_client: OpenSearch):   
    def execute_twitter(query: str, translate=False, ignore={}):
        tweets = get_data(os_client=os_client, 
                                querystring=query, 
                                translate=translate,
                                ignore=ignore)

        to_opensearch(os_client, to_ndjson(tweets))
    return execute_twitter


def full_lang(tweet_lang: str) -> str:
    """Transates the ISO639 Language Code into full name"""
    from iso639 import languages
    lang = "" 
    try:
        if tweet_lang == 'und':
            lang = 'Undefined'
        elif tweet_lang == 'zxx':
            lang = 'No linguistic content'
        elif tweet_lang== 'qme':
            lang = 'Media Links'
        elif tweet_lang == 'qht':
            lang = 'Hashtags'
        elif tweet_lang != '':
            lang = languages.get(part1=tweet_lang).name

    except:
        pass
        
    return lang


def get_marker(os_client: OpenSearch):
    """Get the last indexed tweet ID"""
    query = {
        "fields": ["_id"],
        "sort": [{
                "created_at": {
                    "order": "desc"
                }
            },
            {
                "_score": {
                    "order": "desc"
                }
            }
        ],
        "size": 1
    }
    result = os_client.search(index='tweets*', body=query)

    try:
        return result['hits']['hits'][0]['_id']
    except: 
        return None


#def pipeline_debug(tweet_id):
#    
#    from util import translate_text
#    from html import unescape
#    cl = create_twitter_client()
#    tweet = cl.get_tweet(tweet_id, 
#                         tweet_fields=['id', 'author_id', 'text', 'lang', 'public_metrics', 'created_at', 'entities'],
#                         expansions=['author_id', 'referenced_tweets.id'], 
#                         user_fields=['username'])
#
#    translated = unescape(translate_text(tweet[0].text)['translatedText'])
#    print (translated)