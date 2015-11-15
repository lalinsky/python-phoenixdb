#!/usr/bin/env bash

set -e

HBASE_VERSION=1.1.2
PHOENIX_VERSION=4.6.0-HBase-1.1

export DEBIAN_FRONTEND=noninteractive

echo "> Removing chef and puppet"
sudo apt-get -y remove chef chef-zero puppet puppet-common
sudo apt-get -y autoremove

echo "> Installing java"
sudo apt-get -y update
sudo apt-get -y install wget openjdk-7-jdk

APACHE_MIRROR="$(python -c 'import json, urllib2; a = json.load(urllib2.urlopen("http://www.apache.org/dyn/closer.cgi?as_json=1")); print a["preferred"].rstrip("/")')"
echo "> Using Apache mirror: $APACHE_MIRROR"

if [ ! -d /opt/hbase ]
then
	echo "> Downloading HBase $HBASE_VERSION"
	wget --no-verbose -P /tmp -c -N $APACHE_MIRROR/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz

	echo "> Extracting HBase"
	sudo mkdir /opt/hbase
	sudo chown vagrant:vagrant -R /opt/hbase
	tar xvf /tmp/hbase-$HBASE_VERSION-bin.tar.gz --strip-components=1 -C /opt/hbase
fi

if [ ! -d /opt/phoenix ]
then
	echo "> Downloading Phoenix $PHOENIX_VERSION"
	wget --no-verbose -P /tmp -c -N $APACHE_MIRROR/phoenix/phoenix-$PHOENIX_VERSION/bin/phoenix-$PHOENIX_VERSION-bin.tar.gz

	echo "> Extracting Phoenix"
	sudo mkdir /opt/phoenix
	sudo chown vagrant:vagrant -R /opt/phoenix
	tar xvf /tmp/phoenix-$PHOENIX_VERSION-bin.tar.gz --strip-components=1 -C /opt/phoenix
fi

echo "> Linking Phoenix server JAR file to HBase lib directory"
ln -svfT /opt/phoenix/phoenix-$PHOENIX_VERSION-server.jar /opt/hbase/lib/phoenix-$PHOENIX_VERSION-server.jar

echo "> Setting JAVA_HOME for HBase"
perl -pi -e 's{^\#?\s*export\s*JAVA_HOME\s*=.*$}{export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64}' /opt/hbase/conf/hbase-env.sh

if ! pgrep -f proc_master >/dev/null
then
	echo "> Starting HBase"
	sudo -u vagrant /opt/hbase/bin/start-hbase.sh
fi

if ! pgrep -f proc_phoenixserver >/dev/null
then
	echo "> Starting Phoenix query server"
	sudo -u vagrant /opt/phoenix/bin/queryserver.py start
fi
