#!/bin/bash

cd ~
. .bash_profile
. .bashrc
cd ~/dbmon
export PRD_MODE=OFF
export SEND_MSG_MODE=ON
~/dbmon/rdsmon3.py 
