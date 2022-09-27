# wait for completion as background process - capture PID
kubectl wait --for=condition=complete job/benchmark -n benchmark &
completion_pid=$!

# wait for failure as background process - capture PID
kubectl wait --for=condition=failed job/benchmark -n benchmark && exit 1 &
failure_pid=$!

# capture exit code of the first subprocess to exit
wait -n $completion_pid $failure_pid

# store exit code in variable
exit_code=$?

if (( $exit_code == 0 )); then
  echo "Job completed"
else
  echo "Job failed with exit code ${exit_code}, exiting..."
fi

kubectl delete job benchmark -n benchmark

exit $exit_code