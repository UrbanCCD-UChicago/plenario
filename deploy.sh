#!/usr/bin/env bash

UPDATE_CMDS="cd /home/urbanccd/plenario ; sudo git checkout master; sudo git pull; sudo supervisorctl restart all; exit;"
SSH_SETTINGS="ssh"

cd /home/urbanccd/plenario
sudo git checkout master
sudo git pull
sudo supervisorctl restart all
ssh ubuntu@10.0.1.100 $UPDATE_CMDS exit;