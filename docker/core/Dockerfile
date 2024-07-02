FROM paas/minimal:latest

## running as root
USER root

## shell for RUN cmd purposes
SHELL ["/bin/bash", "-c"]

#####
## ## SYS Package Setup
#####

# LOCALE (important for python, etc.)
RUN localedef -i en_US -f UTF-8 en_US.UTF-8

RUN sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo
RUN sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo
RUN sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/*.repo

ENV LANG="en_US.UTF-8"
ENV LANGUAGE="en_US.UTF-8"
ENV LC_CTYPE="en_US.UTF-8"
ENV LC_NUMERIC="en_US.UTF-8"
ENV LC_TIME="en_US.UTF-8"
ENV LC_COLLATE="en_US.UTF-8"
ENV LC_MONETARY="en_US.UTF-8"
ENV LC_MESSAGES="en_US.UTF-8"
ENV LC_PAPER="en_US.UTF-8"
ENV LC_NAME="en_US.UTF-8"
ENV LC_ADDRESS="en_US.UTF-8"
ENV LC_TELEPHONE="en_US.UTF-8"
ENV LC_MEASUREMENT="en_US.UTF-8"
ENV LC_IDENTIFICATION="en_US.UTF-8"
ENV LC_ALL="en_US.UTF-8"

# Python3 and Env Prereqs
RUN yum update -y \
    && yum install -y \
   autoconf \
   automake \
   binutils \
   bison \
   flex \
   gcc \
   gcc-c++ \
   gettext \
   libtool \
   make \
   patch \
   pkgconfig \
   redhat-rpm-config \
   rpm-build \
   rpm-sign \
   byacc \
   cscope \
   ctags \
   diffstat \
   doxygen \
   elfutils \
   gcc-gfortran \
   git \
   indent \
   intltool \
   patchutils \
   rcs \
   subversion \
   swig \
   systemtap \
    && yum install -y \
        wget \
        python3.x86_64 \
        python3-devel.x86_64 \
        python3-pip.noarch \
        bzip2 \
        glibc.i686 \
        zip \
        unzip \
    && yum clean all \
    && rm -rf /var/cache/yum

# Update base python setup packages (avoids
RUN pip3 install --no-cache-dir --upgrade pip wheel setuptools

#####
## ## Chrome & ChromeDriver Setup
#####

## Installing AWS CLI
RUN curl -LfSo /tmp/awscliv2.zip "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
    && unzip -q /tmp/awscliv2.zip -d /opt \
    && /opt/aws/install

## Getting chrome browser
RUN wget https://dl.google.com/linux/chrome/rpm/stable/x86_64/google-chrome-stable-120.0.6099.109-1.x86_64.rpm -P /tmp/ \
    && yum install /tmp/google-chrome-stable-120.0.6099.109-1.x86_64.rpm -y \
    && rm /tmp/google-chrome-stable-120.0.6099.109-1.x86_64.rpm

## Getting chrome driver
RUN wget --no-check-certificate -O /tmp/chromedriver.zip https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/120.0.6099.109/linux64/chromedriver-linux64.zip \
    && unzip /tmp/chromedriver.zip chromedriver-linux64/chromedriver -d /tmp/ \
    && mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/ \
    && rm -rf /tmp/chromedriver*

#####
## ## Python packages
#####

COPY ./docker/core/minimal-requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

#####
## ## App Setup
#####

## tmpdir/dldir settings
# where temporary files stored by tools like mktemp
ENV TMPDIR="/var/tmp"

# setup workdir
ENV APP_REPO_DIR="/app"
RUN mkdir -p "${APP_REPO_DIR}"

# make sure PATH makes sense
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Default WORKDIR is app setup dir
WORKDIR "${APP_REPO_DIR}"
