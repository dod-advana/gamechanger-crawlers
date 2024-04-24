FROM --platform=linux/amd64 ubuntu:latest

# Update and install necessary packages
RUN apt-get update && apt-get upgrade -y ca-certificates && \
    apt-get install -y curl unzip xvfb libxi6 libgconf-2-4 wget sudo git libxml2-dev libxslt1-dev

# Install Google Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb --no-check-certificate && \
    apt -y install ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb

# Install ChromeDriver
RUN wget https://storage.googleapis.com/chrome-for-testing-public/123.0.6312.105/linux64/chromedriver-linux64.zip --no-check-certificate && \
    unzip chromedriver-linux64.zip -d /usr/local/bin/ && \
    rm chromedriver-linux64.zip

# Install Miniconda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/miniconda && \
    rm Miniconda3-latest-Linux-x86_64.sh

ENV PATH="${PATH}:/opt/miniconda/bin/"

# Create conda environment
RUN conda create -n gc-crawlers python=3.12 -y
RUN echo "source activate gc-crawlers" > ~/.bashrc

# Clone repo
COPY . /home/gamechanger-crawlers

# Install Python dependencies
RUN /bin/bash -c "source activate gc-crawlers && \
                  pip install --upgrade pip setuptools wheel && \
                  pip install -r /home/gamechanger-crawlers/docker/core/minimal-requirements.txt"