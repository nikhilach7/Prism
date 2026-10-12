# Small custom Hadoop image: Ubuntu + JRE 11 + Python 3 (needed by Hadoop Streaming mappers/reducers)
# We build our own because the stock Hadoop images do not reliably ship python3.
FROM eclipse-temurin:11-jre-jammy

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 curl procps ca-certificates \
 && ln -sf /usr/bin/python3 /usr/local/bin/python \
 && rm -rf /var/lib/apt/lists/*

ARG HADOOP_VERSION=3.3.6
RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar -xz -C /opt \
 && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH \
    HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root YARN_NODEMANAGER_USER=root

# JAVA_HOME is already set by the temurin base image; make Hadoop see it explicitly too
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

COPY conf/ /opt/hadoop/etc/hadoop/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /hadoop/dfs/name /hadoop/dfs/data

ENTRYPOINT ["/entrypoint.sh"]
