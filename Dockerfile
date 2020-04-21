FROM centos:8

RUN dnf -y install epel-release && \
    dnf -y upgrade ca-certificates --disablerepo=epel && \
    dnf install -y python3-pip && \
    pip3 install 'elasticsearch>=6.0.0,<7.0.0' 'elasticsearch-dsl>=6.0.0,<7.0.0' htcondor requests

COPY . /monitoring

WORKDIR /monitoring

ENV CONDOR_CONFIG=/monitoring/condor_config
