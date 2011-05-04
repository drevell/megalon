#!/bin/bash

MEGALON_BASE=`dirname $0`
source $MEGALON_BASE/conf/conf.sh
cd $MEGALON_BASE
echo "Running in `pwd`"
java -ea -cp $CP $PROPS org.megalon.Megalon $1 $2 $3 $4 $5 <&- >> $MEGALON_CONSOLE_LOG_FILE 2>&1 &
