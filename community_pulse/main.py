import os
import time
import logging
import argparse
import dotenv
from community_pulse.cluster_init import enforce_index_templates
from community_pulse.util import parse_config, initialize_opensearch_client, init_logging
import community_pulse.twitter

logger = logging.getLogger('community-pulse')
dotenv.load_dotenv()


def get_args():
  def dir_path(string):
    """Helper for path parsing"""
    if os.path.isfile(string):
        return string
    
    elif string == '/etc/pulse/config.yml':
      try:
        os.mkdir("etc", "pulse")
        with open("config.yaml", "rw") as f:
          f.write()
        return string
      except PermissionError:
        raise PermissionError("Could not create config file")
    else:
      raise  FileNotFoundError(string)

  parser = argparse.ArgumentParser(description="""Community-Pulse is a project for collecting \
                                                and aggregating community data.""")
  parser.add_argument('--config',
                      default='/etc/pulse/config.yml', 
                      type=dir_path,
                      help="Directory where the config is located. \
                                  If not passed a templated is paced in /etc/pulse")
  parser.add_argument('--jobs', 
                      nargs='+', 
                      default=[],
                      help="A list of the jobs (defined in the config) \
                                  you would like to run")

  return parser.parse_args()


def job_runner(job_list, full_config):
  jobs_map = {
      'twitter': community_pulse.twitter.gen_twitter_executor()
  }
  if len(job_list)==0:
    job_list = full_config['jobs']
  
  filtered_jobs = { job: full_config['jobs'][job] for job in job_list }

  for job, config in filtered_jobs.items():
    curr_job = jobs_map.get(config['type'])
    start = time.time()
    logger.debug("Starting Job %s", job)
    curr_job(job, **config)
    end = time.time()
    logger.debug("Finshed %s in: %is", job, end - start)
    

def main():
  """Main loop for community pulse"""
  args = get_args()
  full_config = parse_config(args.config)
  
  init_logging(logger, full_config['settings']['log_level'])
  initialize_opensearch_client(full_config['settings']['opensearch'])

  enforce_index_templates()

  job_runner(args.jobs, full_config)

  # Should make this a little smarter
  # create_meta_indices(os_client)
  

if __name__ == "__main__":
  main()
