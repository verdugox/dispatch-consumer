# ---------- Build ----------
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml ./
# cache de dependencias
RUN mvn -q -e -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -e -DskipTests package

# ---------- Runtime ----------
FROM eclipse-temurin:17-jre
WORKDIR /opt/app
COPY --from=build /app/target/dispatch-consumer-*.jar app.jar

# flags opcionales de JVM
ENV JAVA_OPTS=""

# el puerto final del consumer
EXPOSE 8086

ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar app.jar"]
