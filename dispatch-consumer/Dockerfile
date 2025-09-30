# ---------- Build ----------
# Imagen base con Maven + JDK 17 para compilar
FROM maven:3.9.9-eclipse-temurin-17 AS build

# Define el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el pom.xml al contenedor
COPY pom.xml ./

# Descarga dependencias Maven (sin compilar aún) para aprovechar cache
RUN mvn -q -e -DskipTests dependency:go-offline

# Copia el código fuente al contenedor
COPY src ./src

# Compila y empaqueta el proyecto (sin ejecutar tests)
RUN mvn -q -e -DskipTests package


# ---------- Runtime ----------
# Imagen ligera con solo JRE 17 para ejecución
FROM eclipse-temurin:17-jre

# Define directorio de trabajo para la app en runtime
WORKDIR /opt/app

# Copia el JAR generado desde la fase build
COPY --from=build /app/target/card-ops-producer-*.jar app.jar

# Variable de entorno para opciones extra de la JVM (GC, heap, etc.)
ENV JAVA_OPTS=""

# Expone el puerto 8085 (inyectado por SERVER_PORT en docker-compose)
EXPOSE 8085

# Comando de inicio para ejecutar la app con posibles flags
ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar app.jar"]
