#!/bin/bash

MEGALON_BASE=`dirname $0`
source $MEGALON_BASE/conf/conf.sh

echo CP is $CP
java -cp $CP $PROPS org.megalon.multistageserver.EchoServer
