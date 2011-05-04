if [ "$MEGALON_CONF_DIR" == "" ] ; then
    MEGALON_CONF_DIR="$MEGALON_BASE/conf"
fi

if [ "$MEGALON_LOG_FILE" == "" ] ; then
    MEGALON_LOG_FILE="$MEGALON_BASE/log/megalon.log"
fi

if [ "$MEGALON_CONSOLE_LOG_FILE" == "" ] ; then
    # Stdout and stderr will be redirected here
    MEGALON_CONSOLE_LOG_FILE="$MEGALON_BASE/log/console.log"
fi

CP=$MEGALON_CONF_DIR:$MEGALON_CONF_DIR/*:$MEGALON_BASE/lib/*:$MEGALON_BASE/bin/classes:$CLASSPATH
PROPS="-Dmegalon.logfile=$MEGALON_LOG_FILE -Dlog4j.configuration=log4j.properties -Dmegalon.confdir=$MEGALON_CONF_DIR"
