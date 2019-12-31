FROM ubuntu:bionic
RUN apt-get update
RUN apt-get install -y openjdk-11-jre
COPY geode /geode
ENV GEODE_HOME="/geode"
ENV PATH="${GEODE_HOME}/bin:${PATH}"