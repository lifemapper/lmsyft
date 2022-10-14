# syntax=docker/dockerfile:1
FROM python:3.10.0rc2-alpine3.14 as base

LABEL maintainer="Specify Collections Consortium <github.com/specify>"

RUN addgroup -S specify -g 888 \
 && adduser -S specify -G specify -u 888

RUN mkdir -p /home/specify \
 && chown specify.specify /home/specify

RUN mkdir -p /scratch-path/log \
 && mkdir -p /scratch-path/sessions \
 && chown -R specify.specify /scratch-path

WORKDIR /home/specify
USER specify

COPY --chown=specify:specify ./requirements.txt .

RUN python -m venv venv \
 && venv/bin/pip install --no-cache-dir -r ./requirements.txt

COPY --chown=specify:specify ./lmtrex ./lmtrex

# back-end-base == base
#FROM base as back-end-base
#FROM python:3.10.0rc2-alpine3.14 as back-end-base


FROM base as dev-flask

#COPY --chown=specify:specify ./lmtrex ./lmtrex


FROM base as dev-back-end
#FROM back-end-base as dev-back-end

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

# back-end == flask
#FROM back-end-base as back-end
#ENV FLASK_ENV=production
#CMD venv/bin/python -m gunicorn -w 4 --bind 0.0.0.0:5000 ${FLASK_APP}


FROM node:16.10.0-buster as base-front-end

LABEL maintainer="Specify Collections Consortium <github.com/specify>"

USER node
WORKDIR /home/node

COPY --chown=node:node lmtrex/frontend/js_src/package*.json ./
RUN npm install

RUN mkdir dist \
 && chown node:node dist

COPY --chown=node:node lmtrex/frontend/js_src .


FROM base-front-end as front-end

RUN npm run build
