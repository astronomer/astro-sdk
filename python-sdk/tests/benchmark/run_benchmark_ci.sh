python3 -c "import yaml; import json; data = yaml.safe_load(open('./test-connections.yaml'))['connections']; f=open('./test-connections.json', 'w'); f.write(json.dumps({item['conn_id']:item for item in data}))"
airflow connections import ./test-connections.json
GIT_HASH=$(git log -1 --format=%h)
python ./tests/benchmark/analyse.py -b $GIT_HASH -o ./auto_generated_results.md
