FROM almalinux:8

RUN dnf -y install epel-release && \
    yum install -y https://repo.opensciencegrid.org/osg/23-main/osg-23-main-el8-release-latest.rpm && \
    yum install -y osg-ca-certs && \
    dnf install -y python38 python38-pip && \
    dnf clean all && yum clean all && \
    ln -s /usr/bin/python3.8 /usr/bin/python && \
    pip3.8 install --no-cache-dir 'elasticsearch>=6.0.0,<7.0.0' 'elasticsearch-dsl>=6.0.0,<7.0.0' htcondor requests prometheus_client

COPY . /monitoring

WORKDIR /monitoring

ENV CONDOR_CONFIG=/monitoring/condor_config
