FROM openjdk:8-jdk

ARG DIST

VOLUME /opt/continuent/
VOLUME /opt/replicator/
VOLUME /home/tungsten/

RUN apt-get update && apt-get upgrade -y && apt-get install -y ruby ruby-dev puppet
RUN puppet module install puppetlabs-apt --version 2.4.0 && puppet module install puppetlabs-ntp --version 3.3.0 && puppet module install continuent-tungsten --version 1.2.0
RUN puppet apply -e "class { 'tungsten' : installMysql => false, installJava => false, disableFirewall => false, docker => true}"
RUN rm -rf /var/lib/apt/lists/*

COPY ./build/${DIST}.tar.gz /opt/continuent/software/${DIST}.tar.gz
RUN tar zxf /opt/continuent/software/${DIST}.tar.gz && chown -R tungsten:tungsten /opt/continuent/software
