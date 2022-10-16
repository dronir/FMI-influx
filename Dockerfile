FROM python:3.10-alpine
RUN apk update \
    && apk add python3-dev gcc musl-dev \
    && python3 -m pip install poetry


WORKDIR /code
COPY poetry.lock pyproject.toml /code/
COPY FMI_influx /code/FMI_influx

RUN poetry install --only main

CMD ["poetry", "run", "python", "-m", "FMI_influx"]
