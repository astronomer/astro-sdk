retval_complete=1
retval_failed=1
while [[ $retval_complete -ne 0 ]] && [[ $retval_failed -ne 0 ]]; do
  sleep 5
  output=$(kubectl wait --for=condition=failed job/benchmark -n benchmark --timeout=0 2>&1)
  retval_failed=$?
  output=$(kubectl wait --for=condition=complete job/benchmark -n benchmark --timeout=0 2>&1)
  retval_complete=$?
done

kubectl delete job benchmark -n benchmark

if [ $retval_failed -eq 0 ]; then
    echo "Job failed. Please check logs."
    exit 1
fi
