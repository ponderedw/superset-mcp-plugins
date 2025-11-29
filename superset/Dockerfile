FROM apache/superset:4.1.1

USER root

RUN pip install psycopg2-binary Pillow 

COPY requirements-local.txt /app/requirements-local.txt

RUN pip install -r /app/requirements-local.txt 

ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY --chown=1001 ./superset-init.sh /superset-init.sh

COPY ./ /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

RUN pip install superset-chat==0.1.0a23

ENTRYPOINT [ "/superset-init.sh" ]