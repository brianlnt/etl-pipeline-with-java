FROM eclipse-temurin:17-jre

# Set working directory
WORKDIR /app

# Copy the built JAR file
COPY target/etl-pipeline-1.0.0.jar app.jar

# Copy sample data files to the working directory
COPY src/main/resources/sample-data/ /app/src/main/resources/sample-data/

# Expose port
EXPOSE 8080

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"] 