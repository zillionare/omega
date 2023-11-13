if [ $IS_GITHUB ]; then
    echo "runs on github, skipping start_service.sh"
    exit 0
fi

echo "本地测试环境，将初始化redis, minio和influxdb!"

export TZ=Asia/Shanghai
sudo -E apt-get update

echo "初始化redis容器"
sudo docker run -d --name tox-redis -p 6379:6379 redis

echo "初始化influxdb容器"
sudo docker run -d -p 8086:8086 --name tox-influxdb influxdb
sleep 3
sudo docker exec -i tox-influxdb bash -c 'influx setup --username my-user --password my-password --org my-org --bucket my-bucket --token my-token --force'

# echo "初始化minio容器"
# sudo docker run -d -p 9000:9000 -p 9001:9001 --name tox-minio minio/minio server /tmp --console-address ":9001"
