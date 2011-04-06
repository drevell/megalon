#!/bin/bash

MEGALON_BASE=`dirname $0`
source $MEGALON_BASE/conf/conf.sh

java -cp $CP $PROPS org.megalon.Main $1 $2 $3 $4 $5
