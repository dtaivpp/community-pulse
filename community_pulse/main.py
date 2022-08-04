from os import getenv
import dotenv
from community_pulse.cluster_init import enforce_index_templates, create_meta_indices
from community_pulse.util import parse_config, initialize_opensearch_client, init_logging
import community_pulse.twitter
import logging
from logging.handlers import RotatingFileHandler
import time
import argparse

logger = logging.getLogger('community-pulse')
dotenv.load_dotenv()

def main():
    parser = argparse.ArgumentParser(description='Community-Pulse is a project for collecting and aggregating community data.')
    args = parser.parse_args()


    full_config = parse_config('./test_config/community_pulse.yml')
    init_logging(logger, full_config['settings']['log_level'])
    initialize_opensearch_client(full_config['settings']['opensearch'])

    jobs_map = {
        'twitter': community_pulse.twitter.gen_twitter_executor()
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


if __name__=="__main__":
    main()
