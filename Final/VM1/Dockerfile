FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Install ONLY the kafka library (boto3 is removed!)
# Install the high-performance C-based kafka library
RUN pip3 install confluent-kafka numpy boto3

# Set the working directory
WORKDIR /app

# Copy the python script into the container
COPY producer.py .

# Run the python script with unbuffered output (for live logs)
CMD ["python3", "-u", "producer.py"]
