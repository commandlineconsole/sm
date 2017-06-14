#!/bin/bash

#----------------
# Spark Wrapper
#----------------
##
## Spark helper scr to manage the application ${-application.name}
##
## Spark properties are build from the generic.properties in the rpm and the local properties
## in /etc/${-application.name}/local.properties
##
## This can be overridden by setting the environment variable:
##
##  * SPARK_LOCAL_PROPERTIES
##    eg: export SPARK_LOCAL_PROPERTIES=/etc/${-application.name}/conf/local.properties.restore
##
##  * SPARK_GENERIC_PROPERTIES
##    eg: export SPARK_GENERIC_PROPERTIES=/opt/${-application.name}/${project.version}/config/generic.properties.restore

usage ()
{
    echo "$(basename $0):  wrapper to start and deploy Spark applications"
    echo  -e "Usage:\n"
    echo  -e "\tspark-wrapper.sh (deploy|redeploy|undeploy|start|stop)"
    echo  -e "\tspark-wrapper.sh (deploy|redeploy|undeploy|start|stop) STREAM_NAMESPACE(optional)"
}

if [ -z "$1" ]; then
    echo "Error: No action defined"
    usage
    exit 1
fi

ACTION=$1

STREAM_NAMESPACING=""

if [ ! -z "$2" ]; then
    STREAM_NAMESPACING=$2
fi

ARTIFACT=${project.artifactId}
APP=${ARTIFACT%-rpm}
APP_NON_ETL=${APP%-etl}

YARN_USR=yarn
APP_USER=spark
VERSION=${project.version}
ROOT_PATH=/opt/${APP}
SPARK_PATH=/opt/hadoopng/spark
APP_PATH=/opt/${APP}/${VERSION}
APP_PKG=${APP}-${VERSION}-dist.tar.gz
APPLICATION_ID='UNKNOWN'

# JAVA SYSTEM PROPERTIES
if [ -z "$DRIVER_MEMORY" ]; then
    DRIVER_MEMORY="4g"
fi
if [ -z "$EXECUTOR_MEMORY" ]; then
    EXECUTOR_MEMORY="2g"
fi
if [ -z "$EXECUTOR_CORES" ]; then
    EXECUTOR_CORES="2"
fi




export HADOOP_CONF_DIR=$(hadoop classpath)

# Target configuration file to generate and log file
TARGET_CONFIG=${APP_PATH}/config/${APP}.properties.rpm
RUN_JOB_LOG=${APP_PATH}/run-job-rpm.log

if [ -n "$SPARK_LOCAL_PROPERTIES" -o -n "$SPARK_GENERIC_PROPERTIES" ]; then
    TARGET_CONFIG=${APP_PATH}/config/${APP}.properties.custom
    RUN_JOB_LOG=${APP_PATH}/run-job-custom.log
fi


# Use default for some properties if not present in the environment
if [ -z "$SPARK_LOCAL_PROPERTIES" ]; then
    SPARK_LOCAL_PROPERTIES=/etc/${APP_NON_ETL}/conf/local.properties
fi

if [ ! -e "$SPARK_LOCAL_PROPERTIES" ]; then
    echo "Error: $SPARK_LOCAL_PROPERTIES doesn't exist!"
    exit 1
fi

if [ -z "$SPARK_GENERIC_PROPERTIES" ]; then
    SPARK_GENERIC_PROPERTIES=${APP_PATH}/config/generic.properties
elif [ ! -e "$SPARK_GENERIC_PROPERTIES" ]; then
    echo "Error: $SPARK_GENERIC_PROPERTIES doesn't exist!"
    exit 1
fi

localize_config()
{
    # This is building the application property file for Spark,
    # merging the generic properties (managed by the code) to the local properties (managed by hiera)

    echo "Generating application configuration ..."

    mkdir -p ${APP_PATH}
    chmod a+rx ${APP_PATH}
    tar xzf ${ROOT_PATH}/${APP_PKG} -C ${APP_PATH}
    chown -R ${APP_USER}:${APP_USER} ${APP_PATH}
    cat $SPARK_GENERIC_PROPERTIES $SPARK_LOCAL_PROPERTIES > ${TARGET_CONFIG}
    sed -i 's/: /=/g' ${TARGET_CONFIG}
    chmod a+r ${TARGET_CONFIG}

    echo "Application configuration generated at ${TARGET_CONFIG}"
}

hdfs_spark()
{
    echo "Creating Spark Directories"

    su - hdfs -c "hdfs dfs -mkdir -p /var/lib/spark"
    su - hdfs -c "hdfs dfs -mkdir -p /var/log/spark"

    su - hdfs -c "hdfs dfs -chmod 755 /var/lib/spark"
    su - hdfs -c "hdfs dfs -chmod 755 /var/log/spark"

    su - hdfs -c "hdfs dfs -chown ${APP_USER}:${APP_USER} /var/lib/spark"
    su - hdfs -c "hdfs dfs -chown ${APP_USER}:${APP_USER} /var/log/spark"

    su - ${APP_USER} -c "hdfs dfs -mkdir -p /var/lib/spark/spark-warehouse"
    su - ${APP_USER} -c "hdfs dfs -chmod 755 /var/lib/spark/spark-warehouse"

    # dw-etl job output folder
    su - spark -c "hdfs dfs -mkdir -p /apps/MI/dw"

}

distribute()
{
    # Does the lifting of the tar ball to HDFS

    echo "Distributing application..."

    if [ ! -e "${ROOT_PATH}/${APP_PKG}" ]; then
        echo "Application package not present - expected at ${ROOT_PATH}/${APP_PKG}"
        exit 1
    fi

    # Extract package if required
    if [ ! -d "${APP_PATH}" ]; then
        mkdir -p ${APP_PATH}
        tar xzf ${ROOT_PATH}/${APP_PKG} -C ${APP_PATH}
        chown -R ${APP_USER}:${APP_USER} ${APP_PATH}
    fi

    # Create app directory on HDFS if it doesn't exist
    ret=$(su - hdfs -c "hdfs dfs -test -d /apps/MI")
    if [ $? -ne 0 ]; then
        echo "Creating hdfs:///apps/MI"
        su - hdfs -c "hdfs dfs -mkdir /apps/MI"
        su - hdfs -c "hdfs dfs -chmod 755 /apps/MI"
    fi

    ret=$(su - hdfs -c "hdfs dfs -test -d /apps/MI/${APP}")
    if [ $? -ne 0 ]; then
        echo "Creating hdfs:///apps/MI/${APP}"
        su - hdfs -c "hdfs dfs -mkdir /apps/MI/${APP}"
        su - hdfs -c "hdfs dfs -chmod 755 /apps/MI/${APP}"
        su - hdfs -c "hdfs dfs -chown ${APP_USER}:${APP_USER} /apps/MI/${APP}"
    fi

    if [ "$STREAM_NAMESPACING" != "" ]; then

        ret=$(su - hdfs -c "hdfs dfs -test -d /apps/MI/${APP}/${STREAM_NAMESPACING}")
        if [ $? -ne 0 ]; then
            echo "Creating hdfs:///apps/MI/${APP}/${STREAM_NAMESPACING}"
            su - hdfs -c "hdfs dfs -mkdir /apps/MI/${APP}/${STREAM_NAMESPACING}"
            su - hdfs -c "hdfs dfs -chmod 755 /apps/MI/${APP}/${STREAM_NAMESPACING}"
            su - hdfs -c "hdfs dfs -chown ${APP_USER}:${APP_USER} /apps/MI/${APP}/${STREAM_NAMESPACING}"
        fi

        ret=$(su - ${APP_USER} -c "hdfs dfs -test -e /apps/MI/${APP}/${STREAM_NAMESPACING}/${APP_PKG}")

    else
        ret=$(su - ${APP_USER} -c "hdfs dfs -test -e /apps/MI/${APP}/${APP_PKG}")
    fi

    if [ $? -ne 0 ]; then

        if [ "$STREAM_NAMESPACING" != "" ]; then

            # Distribute to HDFS
            echo "Uploading ${ROOT_PATH}/${APP_PKG} to hdfs:///apps/MI/${APP}/${STREAM_NAMESPACING}"
            su - ${APP_USER} -c "hdfs dfs -put ${ROOT_PATH}/${APP_PKG} /apps/MI/${APP}/${STREAM_NAMESPACING}"

            su - ${APP_USER} -c "hdfs dfs -rm -skrash /apps/MI/${APP}/${STREAM_NAMESPACING}/current-dist.tar.gz"
            su - ${APP_USER} -c "hdfs dfs -cp /apps/MI/${APP}/${STREAM_NAMESPACING}/${APP_PKG} /apps/MI/${APP}/${STREAM_NAMESPACING}/current-dist.tar.gz"
        else

            # Distribute to HDFS
            echo "Uploading ${ROOT_PATH}/${APP_PKG} to hdfs:///apps/MI/${APP}"
            su - ${APP_USER} -c "hdfs dfs -put ${ROOT_PATH}/${APP_PKG} /apps/MI/${APP}"

            su - ${APP_USER} -c "hdfs dfs -rm -skrash /apps/MI/${APP}/current-dist.tar.gz"
            su - ${APP_USER} -c "hdfs dfs -cp /apps/MI/${APP}/${APP_PKG} /apps/MI/${APP}/current-dist.tar.gz"
        fi

    fi

    # Generate configuration
    localize_config
}

hdfs_clean()
{
    if [ "$STREAM_NAMESPACING" != "" ]; then
        echo "Cleaning hdfs:///apps/MI/${APP}/${STREAM_NAMESPACING}/${APP_PKG}"
        su - hdfs -c "hdfs dfs -rm -skrash /apps/MI/${APP}/${STREAM_NAMESPACING}/${APP_PKG}"
    else
        echo "Cleaning hdfs:///apps/MI/${APP}/${APP_PKG}"
        su - hdfs -c "hdfs dfs -rm -skrash /apps/MI/${APP}/${APP_PKG}"
    fi
}

local_clean()
{
    echo "Cleaning ${APP_PATH}"
    if [ -z  ${APP_PATH} ]; then
        echo "APP_PATH is empty. Cannot clean"
        exit 1
    else
        rm -fr ${APP_PATH}
    fi
}

start() {
    echo "Starting application..."

    SPARK_JOB_CLASSNAME=$( grep spark.job.class ${APP_PATH}/config/${APP}.properties.rpm | awk -F= '{ print $2 }'  )

    # Submit SPARK Application to Yarn
    su - ${APP_USER} -c "nohup ${SPARK_PATH}/bin/spark-submit \
    --master yarn \
    --name ${ARTIFACT} \
    --class ${SPARK_JOB_CLASSNAME} \
    --driver-memory ${DRIVER_MEMORY} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${APP_PATH}/lib/${APP}-${-application.version}.jar \
    -c ${APP_PATH}/config/${APP}.properties.rpm \
    > ${RUN_JOB_LOG} 2>&1 < /dev/null &"

    if [ $? == 0 ]; then
        echo $$ > /var/run/${ARTIFACT}.pid
    fi
}

get_application_id()
{

    ret=$(grep 'Submitted application application_' ${RUN_JOB_LOG})
    if [ $? -ne 0 ]; then
        APPLICATION_ID="UNKNOWN"
    else
        APPLICATION_ID=$(echo "$ret" | sed -e 's/.* \(application_.*\)$/\1/')
    fi

}

stop()
{

    get_application_id

    if [ "$APPLICATION_ID" == "UNKNOWN" ]; then
        echo "Application ID can't be found"
    else
        echo "Stopping $APPLICATION_ID"
        su - ${YARN_USR} -c "yarn application -kill $APPLICATION_ID"
        echo "Killed $APPLICATION_ID"
        if [ -f /var/run/${ARTIFACT}.pid ]; then
            rm -f /var/run/${ARTIFACT}.pid
        fi
    fi
}


deploy()
{
    echo "Deploying application..."
    distribute
    hdfs_spark
    start
}

redeploy()
{
    echo "Redeploying application..."
    stop
    hdfs_clean
    distribute
    hdfs_spark
    start
}

undeploy()
{
    echo "Undeploying application..."
    stop
    hdfs_clean
    local_clean
}

if [ -z "$ACTION" ]; then
    echo 'Error: ACTION not provided'
    usage
    exit 1
fi

case $ACTION in
    start|deploy)
        deploy
        exit $?
    ;;

    undeploy)
        undeploy
        exit $?
    ;;

    redeploy)
        redeploy
        exit $?
    ;;

    stop)
        stop
        exit $?
    ;;

    *)
        echo "Invalid action: $ACTION"
        usage
        exit 1
    ;;
esac
