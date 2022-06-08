#!/bin/bash
COARSE_FLAG=false
CORES=64
TIMEOUT=300
SVERSION=3.1.1
PYSPARK_CONFIG_FILE_OPT=""
MEMORY_MAX=0
MESOS_MASTER="mesos://zk://10.0.12.77:2181,10.0.12.78:2181,10.0.12.51:2181,10.0.12.60:2181,10.0.12.75:2181,10.0.12.76:2181,10.0.12.18:2181/mesos"
MONGO_API=1
PACKAGES_CMD=""
HIVE_DB_PATH="${HOME}/.hive/"
RUNNING_DIRECTORY="."

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
export SPARK_PYTHON_OPTIONS="--conf spark.pyspark.python=/usr/bin/python3 --conf spark.pyspark.driver.python=/usr/bin/jupyter"
export GPU_MAX=0
NUM_MAX_AGENTS=10
CREDENTIALFILE=""
MEM_WORKER=0

while getopts "C:c:n:T:V:F:M:H:L:2G:N:t:W:D:" flag
  do
	case $flag in
		C)
		  CREDENTIALFILE="${OPTARG}"
		;;
		c)
		  CORES=$OPTARG
		;;
		n)
		  TASKNAME="$OPTARG"
		;;
		T)
		  TIMEOUT=${OPTARG}
		;;
		V)
		  SVERSION=${OPTARG}
		;;
		F)
		  PYSPARK_CONFIG_FILE_OPT="--config=/export/usuarios01/hmolina/.jupyter/${OPTARG}.py"
		;;
		H)
		  MESOS_MASTER="${OPTARG}"
		;;
		t)
		  MEMORY_MAX=${OPTARG}
		;;
		2)
		  export PYSPARK_PYTHON=python2
		  export SPARK_PYTHON_OPTIONS="--conf spark.pyspark.python=/usr/bin/python2 --conf spark.pyspark.driver.python=/usr/bin/jupyter"
		;;
		G)
		  GPU_MAX=${OPTARG}
		  ;;
		N)
		  NUM_MAX_AGENTS=${OPTARG}
		  ;;
		W)
		  MEM_WORKER=${OPTARG}
		  ;;
		D)
		  RUNNING_DIRECTORY="${OPTARG}"
		  ;;
	esac
  done

if [ "${CREDENTIALFILE}X" != "X" ]
then
  SPARK_MESOS_CREDENTIALS=$( cat ${CREDENTIALFILE} )
fi

NUM_MAX_CORES=$(( ${NUM_MAX_AGENTS} * ${CORES} ))
if [ ${MEM_WORKER} -eq 0 ]
then
  MEM_WORKER=$(( 4 * ${CORES} ))
fi

if [ -d /opt/spark-${SVERSION}-bin-2.6.0 ]
then
  SPARK_HOME=/opt/spark-${SVERSION}-bin-2.6.0
  HADOOP_VERSION=2.6.0
elif [ -d /opt/spark-${SVERSION}-bin-2.7.3 ]
then
  SPARK_HOME=/opt/spark-${SVERSION}-bin-2.7.3
  HADOOP_VERSION=2.7.3
elif [ -d /opt/spark-${SVERSION}-bin-2.8.2 ]
then
  SPARK_HOME=/opt/spark-${SVERSION}-bin-2.8.2
  HADOOP_VERSION=2.8.2
elif [ -d /opt/spark-${SVERSION}-bin-2.8.3 ]
then
  SPARK_HOME=/opt/spark-${SVERSION}-bin-2.8.3
  HADOOP_VERSION=2.8.3
else
  printf "Imposible determinar version HADOOP\n"
  exit -1
fi

export SITE_NAME=`uname -n`
SPARK_VH=`echo $SVERSION | cut -d . -f 1 `
SPARK_VL=`echo $SVERSION | cut -d . -f 2 `
HIVE_DB_PATH_M=`uname -n`
HIVE_DB_PATH="${HIVE_DB_PATH}/${HIVE_DB_PATH_M}.$$"

if [ ! -d $HIVE_DB_PATH ]
then
  mkdir -p $HIVE_DB_PATH
fi

HIVE_DB_PATH="file://${HIVE_DB_PATH}"

if [ $SPARK_VH  -lt 2 ]
then
  export PACKAGES_CMD="${PACKAGES_CMD} --packages com.databricks:spark-csv_2.11:1.2.0"
  export PYTHONHASHSEED=1
else
  export PYTHONHASHSEED=0
fi

if [ $MONGO_API -gt 0 ]
then
  export PACKAGES_CMD="${PACKAGES_CMD} --conf spark.mongodb.input.uri=${MONGO_INPUT}"
  export PACKAGES_CMD="${PACKAGES_CMD} --conf spark.mongodb.output.uri=${MONGO_OUTPUT}"
  if [ $SPARK_VH -ge 2 ]
  then
	export PACKAGES_CMD="${PACKAGES_CMD} --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0"
  else 
	export PACKAGES_CMD="${PACKAGES_CMD} --packages org.mongodb.spark:mongo-spark-connector_2.11:1.1.0"
  fi
fi

export MESOS_SECURITY=""
if [ "${SPARK_MESOS_SECRET}X" == "X" ]
then
  if [ "${SPARK_MESOS_CREDENTIALS}X" != "X" ]
  then
	export MESOS_SECURITY="--conf spark.mesos.principal=${USER}"
	if [ "${SPARK_MESOS_SECRET}X" == "X" ]
	then
	  export MESOS_SECURITY="${MESOS_SECURITY} --conf spark.mesos.secret=${SPARK_MESOS_CREDENTIALS}"
	fi
  fi
else
  export MESOS_SECURITY="--conf spark.mesos.principal=${USER}"
fi

export LD_PRELOAD=/opt/intel/composerxe/compiler/lib/intel64/libiomp5.so:/opt/intel/composerxe/mkl/lib/intel64/libmkl_core.so:/opt/intel/composerxe/mkl/lib/intel64/libmkl_rt.so

export SPARK_DRIVER_OPTS="--conf spark.driver.maxResultSize=${MEMORY_MAX} --conf spark.local.dir=/export/workdir/spark/tmp --driver-memory 20G --driver-cores 8 --driver-class-path /etc/hadoop/hdfs-site.xml:/etc/hadoop/core-site.xml:/etc/hadoop --conf spark.sql.warehouse.dir=${HIVE_DB_PATH} --conf spark.mesos.role=${USER}"

export SPARK_EXECUTOR_OPTS="--conf spark.shuffle.io.connectionTimeout=180000 --executor-memory ${MEM_WORKER}G --conf spark.executor.cores=${CORES}  --conf spark.cores.max=${NUM_MAX_CORES} --conf spark.shuffle.service.enabled=true --conf spark.executorEnv.LD_PRELOAD=${LD_PRELOAD} --conf spark.executorEnv.PYTHONHASHSEED=${PYTHONHASHSEED}"

if [ "${GPU_MAX}" -gt 0 ]
then
  export SPARK_EXECUTOR_OPTS="${SPARK_EXECUTOR_OPTS} --conf spark.mesos.gpus.max=${GPU_MAX}"
fi

export SPARK_PARAMETERS="${SPARK_PYTHON_OPTIONS} --conf=spark.eventLog.dir=/var/tmp --conf spark.executor.uri=/opt/spark-dist/spark-${SVERSION}-bin-${HADOOP_VERSION}.tgz ${PACKAGES_CMD} ${MESOS_SECURITY} ${SPARK_EXECUTOR_OPTS} ${SPARK_DRIVER_OPTS} --master ${MESOS_MASTER}"

export LOCAL_SPARK_JAVA_OPTS="--conf spark.shuffle.service.enabled=true --conf spark.rpc.askTimeout=${TIMEOUT} --conf spark.network.timeout=${TIMEOUT}"

export PYSPARK_DRIVER_PYTHON_OPTS="notebook ${PYSPARK_CONFIG_FILE_OPT} --ip='0.0.0.0' --no-browser --notebook-dir='${RUNNING_DIRECTORY}/'"

export SPARKR_SUBMIT_ARGS="${SPARK_PARAMETERS} ${LOCAL_SPARK_JAVA_OPTS} sparkr-shell "




${SPARK_HOME}/bin/pyspark --name "${TASKNAME}:${SITE_NAME}" ${SPARK_PARAMETERS} ${LOCAL_SPARK_JAVA_OPTS}

