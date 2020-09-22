FROM python:3.7-stretch as requirements

ARG NEURO_EXTRAS_VERSION

RUN pip install --user \
    neuro-extras==$NEURO_EXTRAS_VERSION \
    awscli google-cloud-storage

FROM python:3.7-stretch as service

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y
RUN apt-get install rsync -y

COPY --from=requirements /root/.local /root/.local
COPY docker.entrypoint.sh /var/lib/neuro/entrypoint.sh
RUN chmod u+x /var/lib/neuro/entrypoint.sh

WORKDIR /root
ENV PATH=/root/.local/bin:$PATH

RUN neuro-extras init-aliases

ENTRYPOINT ["/var/lib/neuro/entrypoint.sh"]
