import time
import logging
from logging.handlers import RotatingFileHandler
from opensearchpy import OpenSearch

logger = logging.getLogger('community-pulse')
os_client = None

def _check_for_marker(os_client: OpenSearch, marker_name: str):
    """
    UNUSED
    This funcition provides a way to manually check for an ending marker. 
    """
    if not os_client.indices.exists("markers"):
        os_client.indices.create("markers")
    
    if os_client.exists(index="markers", id=marker_name): 
        marker = os_client.get(index="markers", id="twitter")
    else:
        marker = {'_source': {'marker': None}}
    return marker['_source']['marker']


def _set_marker(os_client: OpenSearch,marker_name: str, marker):
    """
    UNUSED
    This funcition provides a way to manually set for an ending marker. 
    """
    marker = {
        "marker": marker
    }
    os_client.index(index="markers", body=marker, id="twitter")


def to_ndjson(_list: list) -> list:
    """Translates list of dicts to ndjson string"""
    import json
    batch = []
    for chunk in _divide_chunks(_list, 200):
        batch.append('\n'.join([json.dumps(item, default=str) for item in chunk]))
    return batch


def _divide_chunks(l: list, n: int):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def to_opensearch(data: list):
    """Sends data to OpenSearch
    
    Parameters: 
    os_client: OpenSearch Client
    data: List of ndjson strings
    """
    os_client = get_os_client()
    for batch in data: 
        os_client.bulk(batch)


def translate_text(text: str):
    """Translates text with Google Cloud Translate
    
    Parameters: 
    text: String to translate
    """
    from google.cloud import translate_v2 as translate
    from html import unescape
    success = False
    sleep_time = 4

    # Instantiates a client
    translate_client = translate.Client()

    while not success:
        try:
            translation = translate_client.translate(text, target_language='en')
            success = True
        except:
            logger.debug(f"Google Translate Exception. Backing off for {sleep_time}s")
            time.sleep(sleep_time)
            sleep_time = backoff(sleep_time)
            
    return unescape(translation['translatedText'])


def parse_config(config='/etc/community-pulse.yml'):
    import yaml
    with open(config, "r") as stream:
        try:
            jobs = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    
    return jobs


def jsonpath_filters(filterlist: dict) -> list:
    from jsonpath_ng import parser
    filters = []

    for key, values in filterlist.items():
        filters.append((parser.parse(key), values))
    
    return filters


def matches_filter(jsonpath_filters, doc) -> bool:
    from jsonpath_ng import jsonpath

    for path, values in jsonpath_filters: 
        matches = [match.value for match in path.find(doc)]
        if any(match in values for match in matches):
            return True
    
    return False


def backoff(current_backoff, max_backoff=360) -> int:
    """Calculates exponential backoff"""
    if not (current_backoff**2) > max_backoff: 
        return (current_backoff**2)
    else: return max_backoff


def initialize_opensearch_client(opensearch_args):
    from opensearchpy import OpenSearch
    global _os_client 
    _os_client = OpenSearch(**opensearch_args)


def get_os_client():
    return _os_client


def init_logging(logger: logging.Logger, log_level):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # File Handler
    fileHandle = RotatingFileHandler('./log/community-pulse', backupCount=15)
    fileHandle.setLevel(logging.DEBUG)
    fileHandle.setFormatter(formatter)

    # Console Handler
    consoleHandle = logging.StreamHandler()
    consoleHandle.setLevel(logging.DEBUG)
    consoleHandle.setFormatter(formatter)

    # Add Handlers
    logger.addHandler(fileHandle)
    logger.addHandler(consoleHandle)

    # Set Level
    logger.setLevel(logging.getLevelName(log_level))


if __name__=="__main__":
    print(parse_config('/home/tippymedia/devel/pulse-beta/test_config/community_pulse.yml'))