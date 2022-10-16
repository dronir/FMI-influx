FROM python:3.10-alpine
RUN apk update \
    && apk add python3-dev gcc musl-dev \
    && python3 -m pip install poetry


WORKDIR /code
COPY poetry.lock pyproject.toml FMI_influx /code/

RUN poetry config virtualenvs.create false \
    && poetry install --no-dev

CMD ["poetry", "run", "python", "-m", "FMI_influx"]