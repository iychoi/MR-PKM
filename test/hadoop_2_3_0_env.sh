#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
export HADOOP_PREFIX=/opt/cdh5.1.3/hadoop-2.3.0-cdh5.1.3
export HADOOP_COMMON_HOME=${HADOOP_PREFIX}
export HADOOP_HDFS_HOME=${HADOOP_PREFIX}
export HADOOP_MAPRED_HOME=${HADOOP_PREFIX}
export HADOOP_YARN_HOME=${HADOOP_PREFIX}
export PATH=${HADOOP_PREFIX}/bin:$PATH
export HADOOP_CONF_DIR=/opt/cdh5.1.3/hadoop-2.3.0-cdh5.1.3/etc/hadoop
