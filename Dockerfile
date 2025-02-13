FROM python:alpine

COPY . /src/
WORKDIR /src/

RUN python3 -m pip install --no-cache ./deps/ascentrade_client-*.tar.gz && \
    python3 -m pip install --no-cache -r requirements.txt && \
    cp .env.template .env

ENTRYPOINT ["python3", "./main.py"]