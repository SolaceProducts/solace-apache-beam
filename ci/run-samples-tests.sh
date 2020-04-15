#!/usr/bin/env bash

set -e

testRecordApp() {
  echo
  echo 'RUN SAMPLE TEST: SolaceRecordTest'
  echo '------------------------------------------------'
  echo

  Q_1="Q/fx-$(tr -cd '[:alnum:]' < /dev/urandom | head -c10)-001"

  curl -X POST -H "content-type:application/json" -u "${SOLACE_MGMT_USERNAME}:${SOLACE_MGMT_PASSWORD}" \
      "https://${SOLACE_HOST}:943/SEMP/v2/config/msgVpns/${SOLACE_VPN_NAME}/queues" \
      -d "{\"queueName\":\"${Q_1}\",\"egressEnabled\":true,\"ingressEnabled\":true,\"permission\":\"delete\"}"

  curl -O https://sftp.solace.com/download/SDKPERF_C_LINUX64
  tar -xvf SDKPERF_C_LINUX64
  pubSubTools/sdkperf_c \
      -cip="tcp://${SOLACE_HOST}:55555" \
      -cu="${SOLACE_USERNAME}@${SOLACE_VPN_NAME}" \
      -cp="${SOLACE_PASSWORD}" \
      -mt=persistent \
      -mn=100 \
      -mr=10 \
      -pfl=README.md \
      -pql="${Q_1}"

  mvn -e compile exec:java \
      -pl solace-apache-beam-samples \
      -Dexec.mainClass=com.solace.apache.beam.examples.SolaceRecordTest \
      -Dexec.args="--output=README10.counts --cip=tcp://${SOLACE_HOST}:55555 --cu=${SOLACE_USERNAME} --vpn=${SOLACE_VPN_NAME} --cp=${SOLACE_PASSWORD} --sql=${Q_1}" \
      2>&1 \
      | tee output.log &

  while [[ ! -s output.log ]] && ! grep -qE 'BUILD SUCCESS|BUILD FAILURE' output.log; do
    echo "$(date) Waiting for compile to complete and runner launch"
    sleep 5
  done

  sleep 20
  grep -E "SolaceRecordTest - \*\*\*CONTRIBUTING. [0-9]+" output.log
  cat output.log
}

if [[ -z "$SOLACE_HOST" ]]; then
  >&2 echo no SOLACE_HOST env found
  exit 1
elif [[ -z "$SOLACE_VPN_NAME" ]]; then
  >&2 echo no SOLACE_VPN_NAME env found
  exit 1
elif [[ -z "$SOLACE_USERNAME" ]]; then
  >&2 echo no SOLACE_USERNAME env found
  exit 1
elif [[ -z "$SOLACE_PASSWORD" ]]; then
  >&2 echo no SOLACE_PASSWORD env found
  exit 1
elif [[ -z "$SOLACE_MGMT_USERNAME" ]]; then
  >&2 echo no SOLACE_MGMT_USERNAME env found
  exit 1
elif [[ -z "$SOLACE_MGMT_PASSWORD" ]]; then
  >&2 echo no SOLACE_MGMT_PASSWORD env found
  exit 1
fi

testRecordApp
