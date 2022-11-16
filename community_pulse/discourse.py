from os import getenv
import requests
from community_pulse.util import get_os_client, logger


def get_data():
  """Fetch Most Recent Discourse Posts"""
  most_recent_id = 1  # get_marker()
  current_id = 2
  base_url = "https://forum.opensearch.org/posts.json"
  posts = []

  logger.debug("Iterating Posts")
  while current_id > most_recent_id:
    posts = requests.get(base_url)
    posts.append()
    print(posts.json())
    #prefix = "?before="
    break


def get_marker():
  """Get the last indexed tweet ID"""
  os_client = get_os_client()
  query = {
      "fields": ["_id"],
      "sort": [
          {"created_at": {"order": "desc"}},
          {"_score": {"order": "desc"}}
      ],
      "size": 1
  }
  result = os_client.search(index='discourse*', body=query)

  try:
    _id = result['hits']['hits'][0]['_id']
    return _id
  except:
    return None


if __name__ == "__main__":
  get_data()
