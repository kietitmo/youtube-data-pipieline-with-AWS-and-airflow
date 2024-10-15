# Use the official Airflow base image
FROM apache/airflow:2.10.2-python3.9

# Copy the requirements.txt file into the container
COPY requirements.txt /requirements.txt

RUN python -m pip install --upgrade pip && \
    pip install -r /requirements.txt

# Create the necessary directory for Kaggle configuration
RUN mkdir -p /home/airflow/.kaggle

USER root
# Copy the kaggle.json file into the container
COPY --chown=airflow:root kaggle.json /home/airflow/.kaggle/kaggle.json

# Set the proper permissions for kaggle.json
RUN chmod 600 /home/airflow/.kaggle/kaggle.json

RUN apt-get update && apt-get install -y unzip

USER airflow

ENV PATH="/home/airflow/.local/bin:$PATH"
