set -x  # print the shell command before being executed
GIT_HASH=$(git log -1 --format=%h)
complete_status=1
failed_status=1
while [[ $complete_status -ne 0 ]] && [[ $failed_status -ne 0 ]]; do
  sleep 60
  output=$(kubectl wait --for=condition=failed job/benchmark-"${GIT_HASH}" -n benchmark --timeout=0 2>&1)
  failed_status=$?
  output=$(kubectl wait --for=condition=complete job/benchmark-"${GIT_HASH}" -n benchmark --timeout=0 2>&1)
  complete_status=$?
done

#kubectl delete job benchmark-"${GIT_HASH}" -n benchmark

if [ $failed_status -eq 0 ]; then
    echo "Job failed. Please check logs."
    exit 1
fi
