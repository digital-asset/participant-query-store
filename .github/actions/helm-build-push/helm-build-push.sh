#! /usr/bin/env bash
set -euo pipefail

# Clear the summary and outputs if there is an error
trap 'rm -vf ${GITHUB_STEP_SUMMARY} ${GITHUB_OUTPUT}' ERR

# Checks if a var is empty
function required() {
  if [ -z "${!1}" ]; then
    echo "'$1' is a required input"
    return 1
  fi
}

# Inputs
inputs=(
  CHART
  REGISTRY
  VERSION
  USERNAME
  PASSWORD
)

failed=false
for input in "${inputs[@]}"; do
  if ! required "${input}"; then
    failed=true
  fi
done

if [ "${failed}" == 'true' ]; then
  exit 1
fi

# Strip leading 'v' from version if present
VERSION="${VERSION#v}"

# Make a temp directory for building
temp="$(mktemp -d)"

# The chart name as it appears in the Chart.yaml file
name="$(yq .name "${CHART}/Chart.yaml")"

package="${temp}/${name}-${VERSION}.tgz"

# Start the summary block
{
  echo '## Helm Build summary'
  echo "_For ${CHART}_"
  echo '### Chart.yaml'
  echo '```yaml'
  cat "${CHART}/Chart.yaml"
  echo '```'
} >>"${GITHUB_STEP_SUMMARY}"

# Check that the release doesn't already exist
if helm show chart --username "${USERNAME}" --password "${PASSWORD}" "oci://${REGISTRY}/${name}:${VERSION}" >/dev/null 2>&1; then
  echo "Package ${name}:${VERSION} already exists in ${REGISTRY}, skipping."
  exit 0
fi

# Build and push the chart
helm package "${CHART}" --version "${VERSION}" --destination "${temp}"

{
  echo '### Package'
  echo '```yaml'
} >>"${GITHUB_STEP_SUMMARY}"

helm push --username "${USERNAME}" --password "${PASSWORD}" "${package}" "oci://${REGISTRY}" 2>&1 | tee -a "${GITHUB_STEP_SUMMARY}"

echo '```' >>"${GITHUB_STEP_SUMMARY}"

# Write the output variable
echo "package=${package}" | tee -a "${GITHUB_OUTPUT}"
