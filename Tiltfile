docker_compose('./lakehouse/docker-compose.yml')

watch_file('./lakehouse')

dc_resource('spark-master', labels=['engine'])
dc_resource('spark-worker', labels=['engine'])
dc_resource('spark-history-server', labels=['engine'])
dc_resource('trino', labels=['engine'])
dc_resource('postgres-metastore', labels=['metastore-database'])
dc_resource('metastore', labels=['metadata'])
dc_resource('jupyter-spark', labels=['query'])

dc_resource('etcd', labels=['metadata'])
dc_resource('minio', labels=['storage'])
dc_resource('standalone', labels=['metadata'])

docker_compose('./prefect/docker-compose.yml')
watch_file('./prefect')

dc_resource('prefect-server', labels=['orchestrator'])
dc_resource('prefect-services', labels=['orchestrator'])
dc_resource('prefect-worker', labels=['orchestrator'])
dc_resource('postgres', labels=['prefect-database'])
dc_resource('redis', labels=['prefect-database'])

local_resource(
  'deploy-prefect-flows',
  'cd ./prefect && prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api" && prefect deploy --all',
  auto_init=True,
  labels=["script"],
  deps=['./prefect/prefect.yaml'],
  resource_deps=['prefect-worker', 'prefect-services', 'prefect-server', 'postgres', 'redis']
)