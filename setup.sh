echo "reading username and password from enviroment file"
source .env


echo "Setting Airflow infrastructure"
rm -r logs
mkdir logs
rm -r temp
mkdir temp

docker-compose up airflow-init
docker-compose up

