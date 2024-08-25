#!/bin/bash

. ~/env.txt
printenv | sort
cd $(dirname "$(readlink -f "$0")")/..
mvn -B -V install -DskipTests -Dmaven.javadoc.skip=true
jabba use ${TEST_JAVA_VERSION}
printenv | sort
mvn -B -V verify -T 1 -Dcassandra.version=${SERVER_VERSION} -Ddse=false -Dmaven.test.failure.ignore=true -Dmaven.javadoc.skip=true;