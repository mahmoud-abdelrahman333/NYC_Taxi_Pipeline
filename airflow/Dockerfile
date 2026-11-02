FROM apache/spark:4.2.0 AS spark

FROM apache/airflow:3.3.2

USER root

COPY --from=spark /opt/spark /opt/spark
COPY --from=spark /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

USER airflow
