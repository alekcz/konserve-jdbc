docker-compose up -d
rm -rf ./temp
rm -rf ./temph2
lein clean
lein cloverage
#lein test :only konserve-jdbc.core-postgres-test
docker-compose down -v