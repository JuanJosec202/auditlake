# Spark 3.5.4 + Iceberg 1.7.1: misma combinación que AWS Glue 5.0.
FROM apache/spark:3.5.4-python3

USER root
ARG MAVEN=https://repo1.maven.org/maven2
ARG ICEBERG_VERSION=1.7.1
ARG MSSQL_JDBC_VERSION=12.8.1.jre11
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar /opt/spark/jars/
RUN ln -s /usr/bin/python3 /usr/local/bin/python \
    && pip install --no-cache-dir pytest==8.3.4 ruff==0.8.6

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/app \
    PATH=/opt/spark/bin:$PATH
WORKDIR /app
USER spark
