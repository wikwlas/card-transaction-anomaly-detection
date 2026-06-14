#!/bin/bash

TO_COMPILE=${1:-true}
TO_COMPILE=$(echo "$TO_COMPILE" | tr '[:upper:]' '[:lower:]')

echo "--- Starting Deployment & Data Generation ---"

# Create required Kafka topics before starting Flink jobs
echo "Ensuring Kafka topics exist..."
docker exec kafka kafka-topics --create --if-not-exists --topic transactions --bootstrap-server localhost:9092
docker exec kafka kafka-topics --create --if-not-exists --topic alerts --bootstrap-server localhost:9092

echo "Waiting for Kafka to propagate topic metadata..."
sleep 5

# Stop existing producer to prevent multiple instances running simultaneously
echo "Checking for running producer.py..."
pkill -f "producer.py" 2>/dev/null
sleep 1

echo "Starting producer.py in the background..."
(cd .. && nohup uv run producer.py > producer.log 2>&1 &)
echo "Producer is running! (Logs are being saved to ../producer.log)"
echo "---------------------------------------------"

# Cancel currently running Flink jobs
echo "Checking for currently running Flink jobs..."
RUNNING_JOBS=$(docker exec flink_jobmanager flink list -r 2>/dev/null | grep "RUNNING" | awk '{print $4}')

if [ -z "$RUNNING_JOBS" ]; then
    echo "No running jobs to cancel."
else
    for JOB_ID in $RUNNING_JOBS; do
        echo "Cancelling Flink job: $JOB_ID"
        docker exec flink_jobmanager flink cancel "$JOB_ID"
    done
    echo "All previous jobs have been cancelled."
    sleep 2
fi

echo "---------------------------------------------"

# Logic for checking the flag and files
if [ "$TO_COMPILE" = "false" ]; then
    JAR_FILE=$(ls target/*.jar 2>/dev/null | grep -v "original" | head -n 1)
    if [ -z "$JAR_FILE" ]; then
        echo "ERROR: to-compile flag is set to False, but no compiled JAR files were found in the target/ directory. You must compile the project first (run without 'false')!"
        exit 1
    else
        echo "to-compile flag = False. Skipping build. Using existing file: $JAR_FILE"
    fi
else
    echo "⏳ Starting Maven build (to-compile = True)..."
    docker run --rm -v "$(pwd):/app" -w /app maven:3.8.5-openjdk-11 mvn clean package

    if [ $? -ne 0 ]; then
        echo "ERROR: Build failed. Check the Maven logs above."
        exit 1
    fi
    echo "Build completed successfully."

    JAR_FILE=$(ls target/*.jar 2>/dev/null | grep -v "original" | head -n 1)
    if [ -z "$JAR_FILE" ]; then
        echo "ERROR: JAR file not found in the target/ directory after build."
        exit 1
    fi
    echo "Using newly built JAR file: $JAR_FILE"
fi

echo "---------------------------------------------"

# Copy the JAR file to the running JobManager container
echo "Copying the file to the flink_jobmanager container..."
docker cp "$JAR_FILE" flink_jobmanager:/tmp/fraud-jobs.jar

# Run the first job (Detection) in detached mode
echo "Starting Flink job: FraudDetector..."
docker exec flink_jobmanager flink run -d -c com.fraud.detector.FraudDetector /tmp/fraud-jobs.jar

# Run the second job (Mongo Ingestion) in detached mode
echo "Starting Flink job: MongoIngestionJob..."
docker exec flink_jobmanager flink run -d -c com.fraud.detector.MongoIngestionJob /tmp/fraud-jobs.jar

echo "Done. All jobs have been submitted to the cluster."
echo "Check the Flink UI at: http://localhost:8081"