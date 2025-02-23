FROM quay.io/jupyter/all-spark-notebook

USER root 
# RUN curl -s https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar -Lo /usr/local/spark/jars/postgresql-42.7.4.jar
RUN curl -s https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar -Lo /usr/local/spark/jars/postgresql-42.7.4.jar
RUN pip install sqlmodel boto3 psycopg2-binary pydantic sqlalchemy