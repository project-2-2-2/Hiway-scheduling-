FROM eclipse-temurin:17-jre

WORKDIR /app
COPY target/wsh-app.jar /app/wsh-app.jar
COPY config /app/config

ENTRYPOINT ["java", "-jar", "/app/wsh-app.jar"]
