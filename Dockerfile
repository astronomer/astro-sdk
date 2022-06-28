FROM dimberman-build
ENV AIRFLOW_HOME=/opt/app/
ENV PYTHONPATH=/opt/app/
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True


RUN mkdir -p $AIRFLOW_HOME
WORKDIR $AIRFLOW_HOME
COPY . .


COPY tests/benchmark/dags $AIRFLOW_HOME/dags
COPY tests/benchmark/run.sh $AIRFLOW_HOME/
COPY tests/benchmark/run.py $AIRFLOW_HOME/
COPY tests/benchmark/config-docker.json $AIRFLOW_HOME/config.json

# Debian Bullseye is shipped with Python 3.9
# Upgrade built-in pip
RUN apt-get update
RUN apt-get install -y python3-pip
RUN apt-get install -y git
RUN apt install -y jq

# Install the Google SDK
RUN apt install -y curl
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y


#WORKDIR $AIRFLOW_HOME
#COPY astro-sdk/src/astro $AIRFLOW_HOME/astro
#COPY /Users/dimberman/.config/gcloud/application_default_credentials.json /tmp/app_creds.json
#ENV GOOGLE_APPLICATION_CREDENTIALS=$AIRFLOW_HOME/app_creds.json

WORKDIR ./tests/benchmark/

#RUN rm -rf astro-sdk
#
#CMD ./run.sh
