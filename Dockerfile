FROM almalinux:8

RUN dnf -y install epel-release && \
    dnf -y upgrade ca-certificates --disablerepo=epel && \
    rpm -i http://dist.eugridpma.info/distribution/igtf/current/accredited/RPMS/ca_COMODO-RSA-CA-1.117-1.noarch.rpm && \
    rpm -i http://dist.eugridpma.info/distribution/igtf/current/accredited/RPMS/ca_InCommon-IGTF-Server-CA-1.117-1.noarch.rpm && \
    dnf install -y python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install 'elasticsearch>=6.0.0,<7.0.0' 'elasticsearch-dsl>=6.0.0,<7.0.0' htcondor requests

COPY . /monitoring

WORKDIR /monitoring

ENV CONDOR_CONFIG=/monitoring/condor_config
