docker-compose up -d
lein test :only konserve-jdbc.core-postgres-test
docker-compose down