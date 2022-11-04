# syntax=docker/dockerfile:1
#############################
# old specify_cache w/ user=specify
#############################
FROM python:3.10.0rc2-alpine3.14 as base

LABEL maintainer="Specify Collections Consortium <github.com/specify>"

RUN addgroup -S specify -g 888 \
 && adduser -S specify -G specify -u 888

RUN mkdir -p /home/specify \
 && chown specify.specify /home/specify

RUN mkdir -p /scratch-path/log \
 && mkdir -p /scratch-path/sessions \
 && mkdir -p /scratch-path/collections \
 && mkdir -p /scratch-path/new_dwcas \
 && mkdir -p /scratch-path/processed_dwcas \
 && mkdir -p /scratch-path/error_dwcas \
 && chown -R specify.specify /scratch-path

WORKDIR /home/specify
USER specify

COPY --chown=specify:specify ./requirements.txt .

RUN python -m venv venv \
 && venv/bin/pip install --no-cache-dir -r ./requirements.txt

COPY --chown=specify:specify ./sppy ./sppy

#############################
# back-end-base == base
#############################

FROM base as dev-flask
# Debug image reusing the base
# Install dev dependencies for debugging
RUN venv/bin/pip install debugpy
# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

ENV FLASK_ENV=development
CMD venv/bin/python -m debugpy --listen 0.0.0.0:${DEBUG_PORT} -m ${FLASK_MANAGE} run --host=0.0.0.0


FROM base as flask

COPY --chown=specify:specify ./flask_app ./flask_app
ENV FLASK_ENV=production
CMD venv/bin/python -m gunicorn -w 4 --bind 0.0.0.0:5000 ${FLASK_APP}


#############################
# old lmtrex aka broker
#############################
#FROM base as dev-back-end
## Debug image reusing the base
## Install dev dependencies for debugging
#RUN venv/bin/pip install debugpy
## Keeps Python from generating .pyc files in the container
#ENV PYTHONDONTWRITEBYTECODE 1
## Turns off buffering for easier container logging
#ENV PYTHONUNBUFFERED 1
#
#ENV FLASK_ENV=development
#CMD venv/bin/python -m debugpy --listen 0.0.0.0:${DEBUG_PORT} -m ${FLASK_MANAGE} run --host=0.0.0.0


#############################
# same as flask
#############################
#FROM base as back-end
##FROM back-end-base as back-end
#ENV FLASK_ENV=production
#CMD venv/bin/python -m gunicorn -w 4 --bind 0.0.0.0:5000 ${FLASK_APP}


FROM node:16.10.0-buster as base-front-end

LABEL maintainer="Specify Collections Consortium <github.com/specify>"

USER node
WORKDIR /home/node

#COPY --chown=node:node lmtrex/frontend/js_src/package*.json ./
COPY --chown=node:node sppy/frontend/js_src/package*.json ./
RUN npm install

RUN mkdir dist \
 && chown node:node dist

#COPY --chown=node:node lmtrex/frontend/js_src .
COPY --chown=node:node sppy/frontend/js_src .


FROM base-front-end as front-end

RUN npm run build
