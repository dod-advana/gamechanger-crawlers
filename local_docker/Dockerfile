FROM --platform=linux/amd64 gc_parser:latest

USER root

SHELL ["/bin/bash", "-c"]

RUN pip install --no-cache-dir --upgrade pip wheel setuptools

COPY docker/core/google-chrome.repo /etc/yum.repos.d/google-chrome.repo
RUN \
    curl https://dl-ssl.google.com/linux/linux_signing_key.pub -o /tmp/google_key.pub \
        && rpm --import /tmp/google_key.pub \
        && rm /tmp/google_key.pub \
    && yum install google-chrome-stable -y --skip-broken \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum -y install wget unzip

RUN \
    wget -O /tmp/chromedriver.zip \
        https://chromedriver.storage.googleapis.com/$(curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE)/chromedriver_linux64.zip \
    && unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/ \
    && rm /tmp/chromedriver.zip

#COPY docker/core/minimal-requirements.txt /tmp/requirements.txt
#RUN pip install --no-cache-dir -r /tmp/requirements.txt

# make sure PATH makes sense
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /gamechanger-crawlers

COPY . .

ENV PYTHONPATH /gamechanger-crawlers

RUN curl -LfSo /tmp/awscliv2.zip "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
    && unzip -q /tmp/awscliv2.zip -d /opt \
    && /opt/aws/install

RUN python -m pip install --no-cache-dir -r docker/core/minimal-requirements.txt

RUN python -m pip install numpy==1.23.1

RUN yum -y install git

ENTRYPOINT ["/bin/bash"]
