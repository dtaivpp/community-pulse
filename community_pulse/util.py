import time
import logging
from logging.handlers import RotatingFileHandler
import json
from opensearchpy import OpenSearch


logger = logging.getLogger('community-pulse')
_OS_CLIENT = None


# def _check_for_marker(os_client: OpenSearch):
#    """
#    UNUSED
#    This funcition provides a way to manually check for an ending marker.
#    """
#    if not os_client.indices.exists("markers"):
#        os_client.indices.create("markers")
#
#    if os_client.exists(index="markers", id=marker_name):
#        marker = os_client.get(index="markers", id="twitter")
#    else:
#        marker = {'_source': {'marker': None}}
#    return marker['_source']['marker']
#
#

# def _set_marker(os_client: OpenSearch, marker):
#    """
#    UNUSED
#    This funcition provides a way to manually set for an ending marker.
#    """
#    marker = {
#        "marker": marker
#    }
#    os_client.index(index="markers", body=marker, id="twitter")


def to_ndjson(_list: list) -> list:
  """Translates list of dicts to ndjson string"""
  batch = []
  for chunk in _divide_chunks(_list, 200):
    batch.append('\n'.join([json.dumps(item, default=str) for item in chunk]))
  return batch


def _divide_chunks(data: list, number: int):
  for i in range(0, len(data), number):
    yield data[i:i + number]


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
      logger.debug(
          "Google Translate Exception. Backing off for %ss", sleep_time)
      time.sleep(sleep_time)
      sleep_time = backoff(sleep_time)

  return unescape(translation['translatedText'])


def parse_config(config='/etc/community-pulse.yml'):
  """Parse the yaml config file"""
  import yaml
  with open(config, "r") as stream:
    try:
      jobs = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
      logger.exception(exc)

  return jobs


def jsonpath_filters(filter_list: dict) -> list:
  """Generates the jsonpath filter list"""
  from jsonpath_ng import parser
  filters = []
  for key, values in filter_list.items():
    filters.append((parser.parse(key), values))
  return filters


def matches_filter(jsonpath_filter, doc) -> bool:
  """Tests to see if any jsonpath matches the provided values"""
  for path, values in jsonpath_filter:
    matches = [match.value for match in path.find(doc)]
    if any(match in values for match in matches):
      #logger.debug("Record Filtered Out: \n%s", doc)
      return True
  return False


def backoff(current_backoff, max_backoff=360) -> int:
  """Calculates exponential backoff"""
  if not (current_backoff**2) > max_backoff:
    return current_backoff**2

  return max_backoff


def initialize_opensearch_client(opensearch_args):
  """Instantiates an instance of the OpenSearch client for reuse"""
  global _OS_CLIENT  # pylint: disable=[W0603]
  _OS_CLIENT = OpenSearch(**opensearch_args)


def get_os_client():
  """Returns Already Instantiated OpenSearch Client"""
  if _OS_CLIENT is None:
    raise Exception("Client not initiated")
  return _OS_CLIENT


def init_logging(_logger: logging.Logger, log_level):
  """Initialize the logging"""
  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

  # File Handler
  file_handle = RotatingFileHandler('./log/community-pulse', backupCount=15)
  file_handle.setLevel(logging.DEBUG)
  file_handle.setFormatter(formatter)

  # Console Handler
  console_handle = logging.StreamHandler()
  console_handle.setLevel(logging.DEBUG)
  console_handle.setFormatter(formatter)

  # Add Handlers
  _logger.addHandler(file_handle)
  _logger.addHandler(console_handle)

  # Set Level
  _logger.setLevel(logging.getLevelName(log_level))


if __name__ == "__main__":
  print(parse_config('/home/tippymedia/devel/pulse-beta/test_config/community_pulse.yml'))