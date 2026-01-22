FROM python:3.12-bullseye

# Set working directory
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cron \
    wget \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip setuptools wheel

COPY requirements.txt /app/requirements.txt
RUN pip3 install -r requirements.txt

COPY requirements_new.txt /app/requirements_new.txt
RUN pip3 install -r requirements_new.txt

COPY . . 

EXPOSE 8000 8501

