FROM tabulario/spark-iceberg


# RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar -Lo /opt/spark/jars/hadoop-common-3.4.1.jar
# RUN curl -s https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.29.38/s3-2.29.38.jar -Lo /opt/spark/jars/s3-2.29.38.jar
# RUN curl -s https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.38/bundle-2.29.38.jar -Lo /opt/spark/jars/bundle-2.29.38.jar
# RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -Lo /opt/spark/jars/hadoop-aws-3.4.1.jar
# RUN curl -s https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar -Lo /opt/spark/jars/postgresql-42.7.4.jar


RUN apt-get update && apt-get install -y wget tar scala
RUN pip install geopy