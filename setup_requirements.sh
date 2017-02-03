#!/bin/bash
# Requirements: ubuntu 16.04

# Mongodb Latest (3.4)

sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6
echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.4.list

sudo apt-get update
sudo apt-get install -y mongodb-org
sudo apt-get install fuse build-essential libcurl4-openssl-dev libxml2-dev \
             libssl-dev libfuse-dev libjson-c-dev pkg-config
sudo service mongod restart

sudo pip install -r requirements.txt

echo "Create a cloudfuse folder to read tracking logs from"
echo "Will only work if you have a ~/.cloudfuse file created with the right credentials"
echo "#cp cloudfuse.sample ~/.cloudfuse"
echo "#chmod 600 /root/.cloudfuse"
echo "#mkdir <mountpath>"
echo "#cloudfuse <mountpath>"

# Add this to  /etc/rc.local
#if test -f /sys/kernel/mm/transparent_hugepage/enabled; then
#  echo never > /sys/kernel/mm/transparent_hugepage/enabled
#fi
#
#if test -f /sys/kernel/mm/transparent_hugepage/defrag; then
#   echo never > /sys/kernel/mm/transparent_hugepage/defrag
#fi
