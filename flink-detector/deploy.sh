#!/bin/bash

echo "Starting containerized Maven build..."

# Run Maven build inside an isolated Docker container
docker run --rm -v "$(pwd):/app" -w /app maven:3.8.5-openjdk-11 mvn clean package

# Check if the build was successful
if [ $? -ne 0 ]; then
    echo "Build failed. Check the Maven logs above."
    exit 1
fi

echo "Build completed successfully."

# Create required Kafka topics before starting Flink jobs
echo "Ensuring Kafka topics exist..."
docker exec kafka kafka-topics --create --if-not-exists --topic transactions --bootstrap-server localhost:9092
docker exec kafka kafka-topics --create --if-not-exists --topic alerts --bootstrap-server localhost:9092

# Find the generated JAR file in the target folder
JAR_FILE=$(ls target/*.jar | grep -v "original" | head -n 1)

if [ -z "$JAR_FILE" ]; then
    echo "JAR file not found in the target/ directory."
    exit 1
fi

echo "Using JAR file: $JAR_FILE"

# Copy the JAR file to the running JobManager container
echo "Copying the file to the flink_jobmanager container..."
docker cp "$JAR_FILE" flink_jobmanager:/tmp/fraud-jobs.jar

# Run the first job (Detection) in detached mode
echo "Starting Flink job: FraudDetector..."
docker exec -it flink_jobmanager flink run -d -c com.fraud.detector.FraudDetector /tmp/fraud-jobs.jar

# Run the second job (Mongo Ingestion) in detached mode
echo "Starting Flink job: MongoIngestionJob..."
docker exec -it flink_jobmanager flink run -d -c com.fraud.detector.MongoIngestionJob /tmp/fraud-jobs.jar

echo "Done. All jobs have been submitted to the cluster."
echo "Check the Flink UI at: http://localhost:8081"