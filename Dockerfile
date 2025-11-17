FROM almalinux:8.9

ARG CLIENT_ID
ARG CLIENT_SECRET
ARG TOKEN_URL

ENV CLIENT_ID=${CLIENT_ID}
ENV CLIENT_SECRET=${CLIENT_SECRET}
ENV TOKEN_URL=${TOKEN_URL}

COPY requirements.txt requirements.txt

RUN dnf -y install epel-release && \
    dnf install -y https://repo.opensciencegrid.org/osg/23-main/osg-23-main-el8-release-latest.rpm && \
    dnf install -y osg-ca-certs && \
    dnf install -y python3.11 python3.11-pip wget tar && \
    dnf clean all && yum clean all && \
    ln -s /usr/bin/python3.11 /usr/bin/python && \
    wget https://github.com/WIPACrepo/rest-tools/archive/refs/tags/v1.8.2.tar.gz && \
    pip3.11 install --upgrade --no-cache-dir -r requirements.txt  ./v1.8.2.tar.gz

COPY . /monitoring

WORKDIR /monitoring

ENV CONDOR_CONFIG=/monitoring/condor_config