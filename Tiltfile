docker_compose('./spark/docker-compose.yml')
watch_file('./spark')

dc_resource('spark-master', labels=['engine'])
dc_resource('spark-worker', labels=['engine'])
dc_resource('spark-history-server', labels=['engine'])

docker_compose('./storage/docker-compose.yml')
watch_file('./storage')

docker_compose('./airflow/docker-compose.yml')
watch_file('./airflow')

docker_compose('./milvus/docker-compose.yml')
watch_file('./milvus')