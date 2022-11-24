ARG BASE_IMAGE="python:3.9-slim-bullseye"
FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && eatmydata apt-get full-upgrade -y \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists /usr/share/locale /usr/share/doc

COPY ./Pipfile ./Pipfile.lock ./
RUN eatmydata pipenv install --system --ignore-pipfile --deploy && \
  rm -rf /root/.cache/pip

COPY ./ /topo_import
WORKDIR /topo_import
