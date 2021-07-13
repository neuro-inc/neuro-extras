FROM neuromation/minimal-base:0.9.7

RUN cd /usr/local/bin && \
     wget https://storage.googleapis.com/kubernetes-release/release/v1.8.0/bin/linux/amd64/kubectl && \
     chmod +x ./kubectl && \
     kubectl version --client
RUN apk add --no-cache rclone

COPY docker.entrypoint.sh /var/lib/neuro/entrypoint.sh

WORKDIR /root

# package version is to be overloaded with exact version
ARG NEURO_EXTRAS_PACKAGE=neuro-extras

RUN pip install --user $NEURO_EXTRAS_PACKAGE
RUN neuro-extras init-aliases

ENTRYPOINT ["/var/lib/neuro/entrypoint.sh"]
