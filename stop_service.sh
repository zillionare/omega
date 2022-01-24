if [ $IS_GITHUB ]; then
    echo "runs on github, skipping stop_service.sh"
    exit 0
fi

echo "将移除start_service脚本启动的本地环境中的redis, postgres, minio和influxdb容器!"

sudo docker rm -f tox-redis
sudo docker rm -f tox-postgres
sudo docker rm -f tox-minio
sudo docker rm -f tox-influxdb

sudo docker ps -a
