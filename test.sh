docker-compose up -d
lein cloverage
#lein test :only konserve-jdbc.core-postgres-test
docker-compose down -v