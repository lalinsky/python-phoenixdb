#!/usr/bin/env bash

set -e

#PHOENIX_VERSION=4.7.0-HBase-1.1
#PHOENIX_VERSION=4.8.2-HBase-1.2
#PHOENIX_VERSION=4.9.0-HBase-1.2
PHOENIX_VERSION=4.10.0-HBase-1.2

case $PHOENIX_VERSION in
4.7.*)
    PHOENIX_NAME=phoenix
    ;;
4.8.*)
    PHOENIX_NAME=apache-phoenix
    ;;
4.9.*)
    PHOENIX_NAME=apache-phoenix
    ;;
4.10.*)
    PHOENIX_NAME=apache-phoenix
    ;;
*)
    echo "! Unsupported Phoenix version - $PHOENIX_VERSION"
    exit 1
;;
esac

case $PHOENIX_VERSION in
*-HBase-1.2)
    HBASE_VERSION=1.2.5
    ;;
*-HBase-1.1)
    HBASE_VERSION=1.1.9
    ;;
*)
    echo "! Unsupported HBase version - $PHOENIX_VERSION"
    exit 1
;;
esac

export DEBIAN_FRONTEND=noninteractive

echo "> Installing java"
sudo apt-get -y update
sudo apt-get -y install wget openjdk-8-jdk-headless python

if [ -z "$APACHE_MIRROR" ]
then
    APACHE_MIRROR="$(python -c 'import json, urllib2; a = json.load(urllib2.urlopen("http://www.apache.org/dyn/closer.cgi?as_json=1")); print a["preferred"].rstrip("/")')"
fi
echo "> Using Apache mirror: $APACHE_MIRROR"

if [ ! -d /opt/hbase ]
then
	echo "> Downloading HBase $HBASE_VERSION"
	wget --no-verbose -P /tmp -c -N $APACHE_MIRROR/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz

	echo "> Extracting HBase"
	sudo mkdir /opt/hbase
	sudo chown ubuntu:ubuntu -R /opt/hbase
	tar xf /tmp/hbase-$HBASE_VERSION-bin.tar.gz --strip-components=1 -C /opt/hbase
fi

if [ ! -d /opt/phoenix ]
then
	echo "> Downloading Phoenix $PHOENIX_VERSION"
	wget --no-verbose -P /tmp -c -N $APACHE_MIRROR/phoenix/$PHOENIX_NAME-$PHOENIX_VERSION/bin/$PHOENIX_NAME-$PHOENIX_VERSION-bin.tar.gz

	echo "> Extracting Phoenix"
	sudo mkdir /opt/phoenix
	sudo chown ubuntu:ubuntu -R /opt/phoenix
	tar xf /tmp/$PHOENIX_NAME-$PHOENIX_VERSION-bin.tar.gz --strip-components=1 -C /opt/phoenix
fi

echo "> Linking Phoenix server JAR file to HBase lib directory"
ln -svfT /opt/phoenix/phoenix-$PHOENIX_VERSION-server.jar /opt/hbase/lib/phoenix-$PHOENIX_VERSION-server.jar

echo "> Setting JAVA_HOME for HBase"
perl -pi -e 's{^\#?\s*export\s*JAVA_HOME\s*=.*$}{export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64}' /opt/hbase/conf/hbase-env.sh

if ! pgrep -f proc_master >/dev/null
then
	echo "> Starting HBase"
	sudo -u ubuntu /opt/hbase/bin/start-hbase.sh
fi

if ! pgrep -f proc_phoenixserver >/dev/null
then
	echo "> Starting Phoenix query server"
	sudo -u ubuntu /opt/phoenix/bin/queryserver.py start
fi
