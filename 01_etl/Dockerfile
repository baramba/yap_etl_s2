FROM python:3.8-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ARG WORKDIR=/home/app
ARG SRC_CODE_DIR=./
ARG POETRY_T=pyproject.toml
ARG POETRY_L=poetry.lock
ARG USER=app
ARG GROUP=apps

WORKDIR ${WORKDIR}

ENV VIRTUAL_ENV=$WORKDIR/venv
ENV PATH="${WORKDIR}/.local/bin:${PATH}"


RUN groupadd -r ${GROUP} && useradd --no-log-init -r -g ${GROUP} ${USER} && \
    chown -R ${USER}:${GROUP} ${WORKDIR} && \
    apt update && apt install -y libpq-dev gcc

USER app

RUN pip install --upgrade pip && pip install poetry==1.1.13 && python -m venv $VIRTUAL_ENV

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --chown=${USER}:${GROUP} ${SRC_CODE_DIR}/${POETRY_T} ./
COPY --chown=${USER}:${GROUP} ${SRC_CODE_DIR}/${POETRY_L} ./


RUN poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi


COPY --chown=${USER}:${GROUP} ${SRC_CODE_DIR} ./

CMD ["python","-m","etltoes.runload"]



