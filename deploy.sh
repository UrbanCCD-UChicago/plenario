#!/usr/bin/env bash

echo $DEPLOY_KEY > pkey.pem
chmod 600 pkey.pem

export SSHPASS=""

UPDATE_CMDS="cd /home/urbanccd/plenario; sudo git checkout master; sudo git pull; sudo supervisorctl restart all; exit;"
SSH_SETTINGS="sshpass -e ssh -o StrictHostKeyChecking=no"
CONN_SETTINGS="-i pkey.pem"

$SSH_SETTINGS ubuntu@plenar.io $CONN_SETTINGS $UPDATE_CMDS
$SSH_SETTINGS ubuntu@52.70.151.225 $CONN_SETTINGS $UPDATE_CMDS

rm pkey.pem