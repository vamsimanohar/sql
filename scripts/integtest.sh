#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.


set -e

function usage() {
    echo ""
    echo "This script is used to run integration tests for plugin installed on a remote OpenSearch/Dashboards cluster."
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Required arguments:"
    echo "None"
    echo ""
    echo "Optional arguments:"
    echo -e "-b BIND_ADDRESS\t, defaults to localhost | 127.0.0.1, can be changed to any IP or domain name for the cluster location."
    echo -e "-p BIND_PORT\t, defaults to 9200 or 5601 depends on OpenSearch or Dashboards, can be changed to any port for the cluster location."
    echo -e "-s SECURITY_ENABLED\t(true | false), defaults to true. Specify the OpenSearch/Dashboards have security enabled or not."
    echo -e "-c CREDENTIAL\t(usename:password), no defaults, effective when SECURITY_ENABLED=true."
    echo -e "-v OPENSEARCH_VERSION\t, no defaults"
    echo -e "-n SNAPSHOT\t, defaults to false"
    echo -e "-h\tPrint this message."
    echo "--------------------------------------------------------------------------"
}

while getopts ":hb:p:s:c:v:n:" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
        b)
            BIND_ADDRESS=$OPTARG
            ;;
        p)
            BIND_PORT=$OPTARG
            ;;
        s)
            SECURITY_ENABLED=$OPTARG
            ;;
        c)
            CREDENTIAL=$OPTARG
            ;;
        v)
            OPENSEARCH_VERSION=$OPTARG
            ;;
        n)
            SNAPSHOT=$OPTARG
            ;;
        :)
            echo "-${OPTARG} requires an argument"
            usage
            exit 1
            ;;
        ?)
            echo "Invalid option: -${OPTARG}"
            exit 1
            ;;
    esac
done


if [ -z "$BIND_ADDRESS" ]
then
  BIND_ADDRESS="localhost"
fi

if [ -z "$BIND_PORT" ]
then
  BIND_PORT="9200"
fi

if [ -z "$SECURITY_ENABLED" ]
then
  SECURITY_ENABLED="true"
fi

if [ -z "$SNAPSHOT" ]
then
  SNAPSHOT="false"
fi

if [ -z "$CREDENTIAL" ]
then
  CREDENTIAL="admin:admin"
fi

USERNAME=`echo $CREDENTIAL | awk -F ':' '{print $1}'`
PASSWORD=`echo $CREDENTIAL | awk -F ':' '{print $2}'`

OS="`uname`"

if [ $OS != "WindowsNT" ]
then
	OPENSEARCH_HOME=`ps -ef | grep -o "[o]pensearch.path.home=\S\+" | cut -d= -f2- | head -n1`
	curl -SL https://raw.githubusercontent.com/opensearch-project/sql/main/integ-test/src/test/resources/datasource/datasources.json -o "$OPENSEARCH_HOME"/datasources.json
	yes | $OPENSEARCH_HOME/bin/opensearch-keystore add-file plugins.query.federation.datasources.config $OPENSEARCH_HOME/datasources.json
	if [ $SECURITY_ENABLED == "true" ] 
	then
		curl -k --request POST --url https://$BIND_ADDRESS:$BIND_PORT/_nodes/reload_secure_settings --header 'content-type: application/json' --data '{"secure_settings_password":""}' --user $CREDENTIAL
	else 
		curl --request POST --url http://$BIND_ADDRESS:$BIND_PORT/_nodes/reload_secure_settings --header 'content-type: application/json' --data '{"secure_settings_password":""}'
	fi
fi

./gradlew integTest -Dopensearch.version=$OPENSEARCH_VERSION -Dbuild.snapshot=$SNAPSHOT -Dtests.rest.cluster="$BIND_ADDRESS:$BIND_PORT" -Dtests.cluster="$BIND_ADDRESS:$BIND_PORT" -Dtests.clustername="opensearch-integrationtest" -Dhttps=$SECURITY_ENABLED -Duser=$USERNAME -Dpassword=$PASSWORD --console=plain
