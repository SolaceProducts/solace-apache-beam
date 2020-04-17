#!/usr/bin/env bash

set -e

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
elif [[ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
  >&2 echo no GOOGLE_APPLICATION_CREDENTIALS env found
  exit 1
elif [[ -z "$DATAFLOW_PROJECT" ]]; then
  >&2 echo no DATAFLOW_PROJECT env found
exit 1
  elif [[ -z "$DATAFLOW_TEMP_ROOT" ]]; then
  >&2 echo no DATAFLOW_TEMP_ROOT env found
  exit 1
fi

echo
echo 'RUN TESTS'
echo '------------------------------------------------'
echo

mvn clean compile verify -Pit

echo
echo 'RUN INTEGRATION TESTS'
echo '------------------------------------------------'
echo

mvn clean compile verify -Pit -DbeamTestPipelineOptions='["--runner=TestDataflowRunner"]'
