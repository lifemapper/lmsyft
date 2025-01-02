#!/bin/bash
sudo apt-get -y update
sudo apt-get -y install docker.io
sudo apt-get -y install docker-compose-v2
git clone --branch 107-automate-AWS-data-workflow https://github.com/specifysystems/sp_network.git
cd sp_network
sudo docker compose -f compose.calc_stats.yml up
sudo shutdown -h now
