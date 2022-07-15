def check_for_marker(os_client ,marker_name: str):
    if not os_client.indices.exists("markers"):
        os_client.indices.create("markers")
    
    if os_client.exists(index="markers", id=marker_name): 
        marker = os_client.get(index="markers", id="twitter")
    else:
        marker = {'_source': {'marker': None}}
    return marker['_source']['marker']


def set_marker(os_client ,marker_name: str, marker):
    marker = {
        "marker": marker
    }
    os_client.index(index="markers", body=marker, id="twitter")


def to_ndjson(_list: list) -> str:
    """Translates list of dicts to ndjson string"""
    import json

    return '\n'.join([json.dumps(item, default=str) for item in _list])


def to_opensearch(os_client, data):
    if not os_client.indices.exists("tweets"):
        os_client.indices.create("tweets")

    os_client.bulk(data)


def translate_text(text):
    from google.cloud import translate_v2 as translate
    from html import unescape

    # Instantiates a client
    translate_client = translate.Client()

    translation = translate_client.translate(text, target_language='en')

    return unescape(translation['translatedText'])


def parse_config(config='/etc/community-pulse.yml'):
    import yaml
    with open(config, "r") as stream:
        try:
            jobs = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    
    return jobs


def create_opensearch_client(opensearch_args):
    from opensearchpy import OpenSearch
    return OpenSearch(**opensearch_args)


def jsonpath_filters(filterlist: dict) -> dict:
    from jsonpath_ng import parser
    filters = {}

    for key, value in filterlist:
        filters.append({parser(key): value})


def matches_filter(jsonpath_filters, tweet) -> bool:
    from jsonpath_ng import jsonpath

    
    for path, values in path_list.items(): 
        matches = [match.value for match in path.find(tweet)]
        if any(values) in matches:
            return True
    
    return False


if __name__=="__main__":
    print(parse_config('/home/tippymedia/devel/pulse-beta/test_config/community_pulse.yml'))