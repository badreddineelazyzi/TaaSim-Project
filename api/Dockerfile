FROM apache/spark:3.5.1

USER root

# Install Python dependencies needed by the API
RUN pip install --no-cache-dir fastapi==0.103.1 uvicorn==0.23.2 pydantic==2.3.0 cassandra-driver==3.28.0 "aiokafka==0.8.1" "kafka-python==2.0.2" python-jose[cryptography]==3.3.0 numpy==1.24.4 pandas==2.0.3

# Download Hadoop AWS JARs for S3A support in PySpark
RUN mkdir -p /opt/spark/jars && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /app

COPY main.py auth.py ./

EXPOSE 8000

USER spark

CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
