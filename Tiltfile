local_resource(
    'init-env',
    'cd ./lakehouse && cp .env.example .env && cd .. && cd ./prefect && cp .env.example .env',
    labels=['setup']
)

docker_compose('./lakehouse/docker-compose.yml')

watch_file('./lakehouse')

dc_resource('namenode', labels=['storage-namenode'])
dc_resource('datanode', labels=['storage-datanode'])
dc_resource('resourcemanager', labels=['storage-resourcemanager'])
dc_resource('nodemanager', labels=['storage-nodemanager'])
dc_resource('spark-master', labels=['engine'])
dc_resource('spark-worker', labels=['engine'])
dc_resource('spark-history-server', labels=['engine'])
dc_resource('trino', labels=['engine'])
dc_resource('postgres-metastore', labels=['metastore-database'])
dc_resource('metastore', labels=['metadata'])
dc_resource('spark-common', labels=['others'])
dc_resource('jupyter-spark', labels=['query'])

docker_compose('./prefect/docker-compose.yml')
watch_file('./prefect')

dc_resource('prefect-server', labels=['orchestrator'])
dc_resource('prefect-services', labels=['orchestrator'])
dc_resource('prefect-worker', labels=['orchestrator'])
dc_resource('postgres', labels=['prefect-database'])
dc_resource('redis', labels=['prefect-database'])

local_resource(
  'deploy-prefect-flows',
  'cd ./prefect && prefect deploy --all',
  auto_init=True,
  labels=["script"],
  deps=['./prefect/prefect.yaml']
)