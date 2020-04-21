FROM python:3.7-stretch as requirements

ARG PIP_EXTRA_INDEX_URL
ARG NEURO_EXTRAS_VERSION

RUN pip install --user \
    neuro-extras==$NEURO_EXTRAS_VERSION


FROM python:3.7-stretch as service

COPY --from=requirements /root/.local /root/.local

WORKDIR /root
ENV PATH=/root/.local/bin:$PATH

RUN neuro-extras init-aliases

ENTRYPOINT ["neuro"]
