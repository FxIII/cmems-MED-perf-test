#!/bin/bash
sudo apt-get -y install curl python-yaml
ln -s /dev/null trash
MOTUURL="http://downloads.sourceforge.net/project/cls-motu/client/motu-client-python/1.0.8/motu-client-python-1.0.8-delivery.tar.gz?r=http%3A%2F%2Fsourceforge.net%2Fprojects%2Fcls-motu%2Ffiles%2Fclient%2Fmotu-client-python%2F1.0.8%2F&ts=1453284151&use_mirror=vorboss"
curl -L "$MOTUURL" | tar -xzvf -
echo 
echo  "******************************************"
echo  "** SETUP USER"
echo  "******************************************"
echo -n "username : "
read uname
echo -n  "password : "
read -s pwd
echo 
echo  auth_mode: cas > auth.yaml
echo user: $uname >> auth.yaml
echo pwd: $pwd >> auth.yaml
