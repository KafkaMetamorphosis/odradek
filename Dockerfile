# Build stage
FROM clojure:temurin-21-lein-alpine AS build

WORKDIR /build

# Cache dependencies before copying source — project.clj changes rarely
COPY project.clj .
RUN lein deps

# Copy source and resources
COPY src/ src/
COPY resources/ resources/

RUN lein uberjar

# Runtime stage
FROM eclipse-temurin:21-jre-alpine AS runtime

LABEL org.opencontainers.image.name="odradek"
LABEL org.opencontainers.image.description="Kafka SLO metrics exporter"

WORKDIR /app

# Create non-root user and group
RUN addgroup -S -g 1001 odradek && \
    adduser -S -u 1001 -G odradek odradek

COPY --from=build /build/target/uberjar/odradek-0.1.0-SNAPSHOT-standalone.jar /app/odradek.jar

# Empty by default — if set, ConfigComponent reads config from this filesystem path.
# In Kubernetes, point this at a ConfigMap volume-mounted file, e.g. /etc/odradek/config.json.
# When unset, the component falls back to the classpath resource config.json (local dev).
ENV CONFIG_PATH=""

EXPOSE 8082

USER odradek

CMD ["java", \
     "-Dfile.encoding=UTF-8", \
     "-jar", "/app/odradek.jar"]
