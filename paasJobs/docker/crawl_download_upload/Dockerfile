FROM advana/gc-crawler:latest

#####
## ## App Setup
#####

## Installing AWS CLI
RUN curl -LfSo /tmp/awscliv2.zip "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
    && unzip -q /tmp/awscliv2.zip -d /opt \
    && /opt/aws/install

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

# copy code
COPY . "${APP_REPO_DIR}"
ENV PYTHONPATH="${APP_REPO_DIR}"

ENTRYPOINT ["/bin/bash", "/app/paasJobs/docker/crawl_and_download/run_job.sh"]