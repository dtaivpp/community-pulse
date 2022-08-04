# Community Pulse

Welcome to community pulse! This project aims to be a great way to keep a pulse on your community. At the moment we are starting with and optomizing for Twitter however the goal is to integrate with everywhere you have social. 

# Getting Started 

Getting started is fairly straight forward.

1. Clone the repo to the directory you want it to run in. 
2. Install the requirements `python -m pip install -r requirements.txt`
3. Run the OpenSearch docker compose (if not using an already stood up instance)
    - `cd infra`
    - `docker-compose --env-file env up`
4. Create a .env file in the root of the repo with the required variables
    - `TW_BEARER_TOKEN=<token here>`
5. Create a cron task to run the automation intermittently 
    - `crontab -e 0 * * * *  cd <working dir> && <path to python> <path to main.py>`
6. *Optional:* Setup translation
    - Setup the authentication for Googles Translation API: https://cloud.google.com/translate/docs/setup
