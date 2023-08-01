# Base Image
FROM python:3.6

# Set environment variables
ENV PYTHONUNBUFFERED 1
ENV CRAWLER_DATA_ROOT /data

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    xvfb \
    libxi6 \
    libgconf-2-4 \
    chromium \
    && curl https://chromedriver.storage.googleapis.com/2.41/chromedriver_linux64.zip -o /tmp/chromedriver.zip \
    && unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/ \
    && apt-get clean

# Set work directory
WORKDIR /app
ENV PYTHONPATH /app

# Install python dependencies
COPY ./docker/core/minimal-requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r minimal-requirements.txt

# Copy project
COPY . .

CMD ["sh", "-c", "scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/executive_orders_spider.py -a download_output_dir='$CRAWLER_DATA_ROOT' -a previous_manifest_location='$CRAWLER_DATA_ROOT/prev-manifest.json' -o '$CRAWLER_DATA_ROOT/output.json'"]
