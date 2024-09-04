# Używamy obrazu Bitnami Spark jako podstawy
FROM bitnami/spark:latest
WORKDIR /app
USER root
# Skopiuj plik JAR do kontenera
COPY target/scala-2.13/spark-scala-yt-app_2.13-0.1.0-SNAPSHOT.jar /app/yt-spark-app.jar
COPY target/mysql-connector-j-8.0.33-kopia.jar /opt/bitnami/spark/jars
ENV HOME=/root

RUN mkdir -p /root/.ivy2/local
ENTRYPOINT [ "/opt/bitnami/spark/bin/spark-submit" ]
CMD [ "--class", "Main", "/app/yt-spark-app.jar" ]
