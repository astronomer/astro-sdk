status_complete=1
status_failed=1
while [[ $status_complete -ne 0 ]] && [[ $status_failed -ne 0 ]]; do
    sleep 5
    output=$(kubectl wait --for=condition=failed job/benchmark --timeout=0 2>&1)
    status_failed=$?
    output=$(kubectl wait --for=condition=complete job/benchmark --timeout=0 2>&1)
    status_complete=$?
done

if [ $status_failed -eq 0 ]; then
    exit 1
fi