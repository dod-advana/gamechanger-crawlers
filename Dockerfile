ARG BASE_IMAGE=registry.access.redhat.com/ubi8/ubi:8.5
ARG BUILDER_BASE_IMAGE="registry.access.redhat.com/ubi8/python-38"
FROM ${BASE_IMAGE} as base-image
# non-root app USER/GROUP
ARG APP_UID=1000
ARG APP_GID=1000
## running as root
USER root
# ensure user/group exists, formally
RUN ( (getent group $APP_GID &> /dev/null) \
    || groupadd --system --gid $APP_GID app_user \
    ) && ( (getent passwd $APP_UID &> /dev/null) \
    || useradd --system --shell /sbin/nologin --gid $APP_GID --uid $APP_UID app_user \
    )
ENV APP_REPO_DIR="/app"
RUN mkdir -p "${APP_REPO_DIR}" \ 
    && chown -R ${APP_UID}:${APP_GID} ${APP_REPO_DIR}

#####
## ## SYS Package Setup
#####

# LOCALE (important for python, etc.) (https://access.redhat.com/solutions/5211991)
RUN sed -i 's/^LANG=.*/LANG="en_US.utf8"/' /etc/locale.conf
ENV LANG="en_US.utf8" 
ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    # poetry
    # https://python-poetry.org/docs/configuration/#using-environment-variables
    POETRY_VERSION=1.0.3 \
    # make poetry install to this location
    POETRY_HOME="/opt/poetry" \
    # make poetry create the virtual environment in the project's root
    # it gets named `.venv`
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    # do not ask any interactive question
    POETRY_NO_INTERACTION=1 \
    \
    # paths
    # this is where our requirements + virtual environment will live
    PYSETUP_PATH="/app" \
    VENV_PATH="/app/.venv"


# prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
# Python3 and Env Prereqs
# ensure RHEL host repos are enabled (undo what's done here https://repo1.dso.mil/dsop/redhat/ubi/ubi8/-/blob/development/Dockerfile#L22)
RUN sed -i "s/enabled=0/enabled=1/" /etc/dnf/plugins/subscription-manager.conf\
    && yum update -y \
    && yum install -y \
    git \
    swig \
    && yum install -y \
    glib2 \
    file \
    wget \
    python38 \
    cairo \
    unzip \
    && yum clean all \
    && rm -rf /var/cache/yum

#####
## ## Chrome & ChromeDriver Setup
#####

# install chrome browser and clamav
COPY config/gc-*.repo /etc/yum.repos.d/
RUN curl https://dl-ssl.google.com/linux/linux_signing_key.pub -o /etc/pki/rpm-gpg/RPM-GPG-KEY-GOOGLE-8 \
    && curl https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-8 -o /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-8 \
    && yum install -y \
    google-chrome-stable \
    xdg-utils \
    liberation-fonts \
    clamav \
    clamav-update \
    && yum clean all \
    && rm -rf /var/cache/yum \
    && rm /etc/pki/rpm-gpg/*-8 \
    && rm /etc/yum.repos.d/gc-*.repo
# install chromedriver
RUN \
    wget -O /tmp/chromedriver.zip \
    https://chromedriver.storage.googleapis.com/$(curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE)/chromedriver_linux64.zip \
    && unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/ \
    && rm /tmp/chromedriver.zip

RUN freshclam --update-db=daily

## tmpdir/dldir settings
# where temporary files stored by tools like mktemp
ENV TMPDIR="/var/tmp"


# https://github.com/python-poetry/poetry/discussions/1879#discussioncomment-216865
# FROM ${BUILDER_BASE_IMAGE} as builder
FROM base-image as builder
ARG APP_UID=1000
ARG APP_GID=1000
# install poetry - respects $POETRY_VERSION & $POETRY_HOME
RUN pip3 install poetry

# copy project requirement files here to ensure they will be cached.
WORKDIR $PYSETUP_PATH
COPY --chown=$APP_UID:$APP_GID . .

# install runtime deps - uses $POETRY_VIRTUALENVS_IN_PROJECT internally
RUN poetry install --no-dev


FROM base-image AS crawler-prod
ARG APP_UID=1000
ARG APP_GID=1000

# Default WORKDIR is app setup dir
WORKDIR "${APP_REPO_DIR}"
COPY --from=builder $PYSETUP_PATH $PYSETUP_PATH
# thou shall not root 
USER $APP_UID:$APP_GID
ENTRYPOINT [ "gc" ]

FROM builder as crawler-dev
ARG APP_UID=1000
ARG APP_GID=1000
# install dev dependencies
RUN poetry install
# add/trust CA list and other dev-specific stuff here
COPY config/trusted-ca-certs/*.pem /etc/pki/ca-trust/source/anchors/
# perform system and python3 CA trust update
RUN update-ca-trust \
    && cat /etc/pki/ca-trust/source/anchors/* >> `python3 -c 'import certifi; print(certifi.where())'`

# AWS CLI
RUN curl -LfSo /tmp/awscliv2.zip "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
    && unzip -q /tmp/awscliv2.zip -d /opt \
    && /opt/aws/install 

# thou shall not root 
USER $APP_UID:$APP_GID
ENTRYPOINT [ "gc" ]
