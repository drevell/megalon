MEGALON_BASE=`dirname $0`
source $MEGALON_BASE/conf/conf.sh

java -ea -cp $CP $PROPS org.megalon.test.TestPreparer $@
