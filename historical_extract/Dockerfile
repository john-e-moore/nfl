# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
ADD . /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get purge -y --auto-remove gcc python3-dev

# Create and activate a virtual environment
RUN python3 -m venv extraction_env
ENV PATH="/app/extraction_env/bin:$PATH"

# Install python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Specify the command to run when the container starts
ENTRYPOINT ["python", "src/main.py"]
