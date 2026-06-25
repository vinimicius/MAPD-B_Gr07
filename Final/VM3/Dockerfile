FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

# Install Python, pip, and the mandatory C-build dependencies
RUN apt-get update && apt-get install -y python3 python3-pip librdkafka-dev && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Install the high-performance C-based kafka library and Dask ecosystem directly
RUN pip3 install confluent-kafka numpy scipy dask distributed

# Copy the processing script
COPY processing.py .

# Run the script with unbuffered output for live terminal logs
CMD ["python3", "-u", "processing.py"]