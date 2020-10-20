FROM python:3.7-stretch as requirements

RUN pip install --user \
    awscli google-cloud-storage crcmod

FROM python:3.7-stretch as service

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
        | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update -y

RUN apt-get install -y --no-install-recommends google-cloud-sdk gcc python3-dev python3-setuptools && \
    curl https://rclone.org/install.sh | bash && \
    apt-get clean -y -qq && \
    apt-get autoremove -y -qq && \
    rm -rf /var/lib/apt/lists/*

COPY --from=requirements /root/.local /root/.local
COPY docker.entrypoint.sh /var/lib/neuro/entrypoint.sh
RUN chmod u+x /var/lib/neuro/entrypoint.sh

WORKDIR /root
ENV PATH=/root/.local/bin:$PATH


ARG NEURO_EXTRAS_VERSION
ARG NEURO_EXTRAS_PACKAGE="neuro-extras==$NEURO_EXTRAS_VERSION"
RUN pip install --user $NEURO_EXTRAS_PACKAGE

RUN neuro-extras init-aliases


ENTRYPOINT ["/var/lib/neuro/entrypoint.sh"]