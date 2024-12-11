# syntax=docker/dockerfile:1
# ........................................................
# Backend base image
#FROM python:3.12.3-alpine3.20 as base
FROM python:3.12-slim-bookworm as base

LABEL maintainer="Specify Network <github.com/specifysystems>"

RUN apt-get update
RUN apt-get install -y awscli
RUN apt-get install -y gcc
RUN apt-get install -y git
RUN apt-get install -y vim

RUN addgroup --gid 888 specify \
 && adduser --ingroup specify --uid 888 specify

RUN mkdir -p /home/specify \
 && chown specify.specify /home/specify

RUN mkdir -p /scratch-path/log \
 && mkdir -p /scratch-path/sessions \
 && chown -R specify.specify /scratch-path

WORKDIR /home/specify
USER specify

COPY --chown=specify:specify ./requirements.txt .

RUN python3 -m venv venv \
 && venv/bin/pip install --upgrade pip \
 && venv/bin/pip install --no-cache-dir -r ./requirements.txt

# This assumes that the sp_network repository is present on the host machine and \
# docker is run from the top of directory (with span directly below).
COPY --chown=specify:specify ./sppy ./sppy


# ........................................................
# Development flask image from base
FROM base as dev-flask
# Install dev dependencies for debugging
RUN venv/bin/pip install debugpy

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1
ENV FLASK_ENV=development
CMD venv/bin/python -m debugpy --listen 0.0.0.0:${DEBUG_PORT} -m ${FLASK_MANAGE} run --host=0.0.0.0


# ........................................................
# Production flask image from base
FROM base as flask

COPY --chown=specify:specify ./flask_app ./flask_app
ENV FLASK_ENV=production
CMD venv/bin/python -m gunicorn -w 4 --bind 0.0.0.0:5000 ${FLASK_APP}


# ........................................................
# Frontend base image (for development)
FROM node:16.10.0-buster as base-front-end

LABEL maintainer="Specify Collections Consortium <github.com/specify>"

USER node
WORKDIR /home/node

COPY --chown=node:node sppy/frontend/js_src/package*.json ./
RUN npm install

RUN mkdir dist \
 && chown node:node dist

COPY --chown=node:node sppy/frontend/js_src .


# ........................................................
# Frontend image (for production) from base-front-end
FROM base-front-end as front-end

RUN npm run build
