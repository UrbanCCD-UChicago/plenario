#!/usr/bin/env bash
# Script that activates plenario environment,
# and runs update script

TIME=$1

source /home/urbanccd/.virtualenvs/plenario/bin/activate
cd /home/urbanccd/plenario
python update.py $TIME