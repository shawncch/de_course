# First-time build can take upto 10 mins.

FROM apache/airflow:3.0.2

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
RUN apt-get install -y wget gzip
# git gcc g++ -qqq

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=470.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"


RUN CLOUD_SDK_VERSION=470.0.0 \
    && DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "/home/google-cloud-sdk" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "/home/google-cloud-sdk" --strip-components=1 \
    && "/home/google-cloud-sdk/install.sh" --bash-completion=false --path-update=false --usage-reporting=false --quiet \
    && rm -rf "${TMP_DIR}" \
    && /home/google-cloud-sdk/bin/gcloud --version

WORKDIR $AIRFLOW_HOME

COPY scripts scripts

#make it executable
RUN chmod +x scripts 


USER $AIRFLOW_UID