# Image Spark cua do an = apache/spark goc + 2 thu con thieu:
#   - numpy, pandas, pyarrow: ban python3 cua image goc KHONG kem, ma pyspark.ml
#     (MLlib, ml.functions.vector_to_array) bat buoc can.
#   - connector Elasticsearch (elasticsearch-spark-30) dat san trong /opt/spark/jars:
#     pom cua ban 8.19.x tren Maven tro toi artifact chua publish nen
#     `--packages` khong resolve duoc; jar nay da shade du dependency nen
#     chi can tai mot file.
ARG SPARK_IMAGE_TAG
FROM apache/spark:${SPARK_IMAGE_TAG}
ARG ES_SPARK_VERSION
USER root
RUN python3 -m pip install --no-cache-dir numpy==1.26.4 pandas==2.2.2 pyarrow==15.0.2
ADD https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/${ES_SPARK_VERSION}/elasticsearch-spark-30_2.12-${ES_SPARK_VERSION}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/elasticsearch-spark-30_2.12-*.jar
