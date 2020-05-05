FROM hashicorp/terraform:0.12.24

RUN \
    apk add --no-cache \
        bash \
        make \
        python3 \
        zip \
        docker \
        openrc \
    && pip3 install awscli

RUN rc-update add docker boot

COPY scripts/* /usr/local/bin/