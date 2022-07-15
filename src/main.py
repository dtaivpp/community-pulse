from os import getenv
import dotenv
from opensearchpy import OpenSearch
from cluster_init import enforce_index_templates, create_meta_indices
from util import parse_config, create_opensearch_client
import twitter

dotenv.load_dotenv()
#os_un = getenv("OPENSEARCH_USER")
#os_pw = getenv("OPENSEARCH_PASSWORD")


# OpenSearch Client
os_client = OpenSearch(
    ['https://admin:admin@localhost:9200'],
    use_ssl=True,
    verify_certs=False)

jobs_map = {
    'twitter': twitter.gen_twitter_executor(os_client)
}


if __name__=="__main__":
    full_config = parse_config('./test_config/community_pulse.yml')
    
    os_client = create_opensearch_client(full_config['settings']['opensearch'])

    enforce_index_templates(os_client)
    create_meta_indices(os_client)

    for job, config in full_config['jobs'].items():
        curr_job = jobs_map.get(job)
        curr_job(**config)
