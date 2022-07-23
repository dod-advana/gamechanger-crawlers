FROM centos:7

# Install EPEL & IUS repos
RUN \
    curl -k -o /tmp/epel.rpm https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm \
    && yum install -y /tmp/epel.rpm \
    && rm -f /tmp/epel.rpm

# AV
RUN \
    yum update -y \
	&& yum install -y file clamav unzip zip wget curl python3 \
	&& yum clean all

# AWS CLI
RUN curl -LfSo /tmp/awscliv2.zip "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
    && unzip -q /tmp/awscliv2.zip -d /opt \
    && /opt/aws/install


ENV AWS_DEFAULT_REGION=us-gov-west-1

ENTRYPOINT ["/srv/dlp-scanner/dlp-scanner.sh"]
