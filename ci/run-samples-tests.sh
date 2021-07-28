#!/usr/bin/env bash

set -e

createQueue() {
  QUEUE="$1"
  SOLACE_MGMT_URI="${SOLACE_MGMT_HOST}/SEMP/v2"
  curl -X POST -H "content-type:application/json" -u "${SOLACE_MGMT_USERNAME}:${SOLACE_MGMT_PASSWORD}" \
      "${SOLACE_MGMT_URI}/config/msgVpns/${SOLACE_VPN_NAME}/queues" \
      -d "{\"queueName\":\"${QUEUE}\",\"egressEnabled\":true,\"ingressEnabled\":true,\"permission\":\"delete\"}"

  echo "echo Deleting queue ${QUEUE}" >> "$CI_CLEANUP"
  echo "curl -X DELETE -u \"${SOLACE_MGMT_USERNAME}:${SOLACE_MGMT_PASSWORD}\" ${SOLACE_MGMT_URI}/config/msgVpns/${SOLACE_VPN_NAME}/queues/${QUEUE}" >> "$CI_CLEANUP"
}

publishMessages() {
  PQL="$1"
  MSG_NUM="${2:-100}"
  MSG_RATE="${3:-10}"

  curl -O https://sftp.solace.com/download/SDKPERF_C_LINUX64
  tar -xvf SDKPERF_C_LINUX64
  pubSubTools/sdkperf_c \
      -cip="${SOLACE_HOST}" \
      -cu="${SOLACE_USERNAME}@${SOLACE_VPN_NAME}" \
      -cp="${SOLACE_PASSWORD}" \
      -mt=persistent \
      -mn="${MSG_NUM}" \
      -mr="${MSG_RATE}" \
      -pfl=README.md \
      -pql="${PQL}"
}

testRecordApp() {
  echo
  echo 'RUN SAMPLE TEST: SolaceRecordTest'
  echo '------------------------------------------------'
  echo

  QUEUE="Q-fx-$(tr -cd '[:alnum:]' < /dev/urandom | head -c10)-001"

  createQueue "$QUEUE"
  publishMessages "$QUEUE"

  RUNNING_PID="testRecordApp-proc.pid"
  mvn -e compile exec:java \
      -pl solace-apache-beam-samples \
      -Dexec.mainClass=com.solace.connector.beam.examples.SolaceRecordTest \
      -Dexec.args="--cip=${SOLACE_HOST} --cu=${SOLACE_USERNAME} --vpn=${SOLACE_VPN_NAME} --cp=${SOLACE_PASSWORD} --sql=${QUEUE}" \
      2>&1 | tee output.log &
  echo $! > "$RUNNING_PID"

  for attempts in {1..120}; do
    if [[ -s output.log ]]; then
      if grep -q 'BUILD FAILURE' output.log; then
        cat output.log
        >&2 echo Failed to start SolaceTestRecord
        exit 1
      elif grep -qE "SolaceRecordTest - \*\*\*CONTRIBUTING. [0-9]+" output.log; then
        break
      fi
    fi

    if [[ "$attempts" == "120" ]]; then
      cat output.log
      >&2 echo Timed out while testing SolaceRecordTest
      kill -9 "$(cat "$RUNNING_PID")"
      exit 1
    fi

    echo "$(date) Waiting for output"
    sleep 5
  done

  kill -9 "$(cat "$RUNNING_PID")"
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
elif [[ -z "$SOLACE_MGMT_HOST" ]]; then
  >&2 echo no SOLACE_MGMT_HOST env found
  exit 1
elif [[ -z "$SOLACE_MGMT_USERNAME" ]]; then
  >&2 echo no SOLACE_MGMT_USERNAME env found
  exit 1
elif [[ -z "$SOLACE_MGMT_PASSWORD" ]]; then
  >&2 echo no SOLACE_MGMT_PASSWORD env found
  exit 1
fi

mvn clean install -pl beam-sdks-java-io-solace
testRecordApp
