from os import getenv
import dotenv
from opensearchpy import OpenSearch
from cluster_init import enforce_index_templates, create_meta_indices
from util import parse_config, initialize_opensearch_client, init_logging
import twitter
import logging
from logging.handlers import RotatingFileHandler
import time

logger = logging.getLogger('community-pulse')
dotenv.load_dotenv()

if __name__=="__main__":
    full_config = parse_config('./test_config/community_pulse.yml')
    init_logging(logger, full_config['settings']['log_level'])
    initialize_opensearch_client(full_config['settings']['opensearch'])

    jobs_map = {
        'twitter': twitter.gen_twitter_executor()
    }

    # Should make this a little smarter
    enforce_index_templates()
    #create_meta_indices(os_client)

    for job, config in full_config['jobs'].items():
        curr_job = jobs_map.get(job)
        start = time.time()
        logger.debug(f"Starting Job {job}")
        curr_job(**config)
        end = time.time()
        logger.debug(f"Finshed {job} in: {end - start}s")
