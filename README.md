## Persistence POC

To start the project run 
```shell script
docker-compose up
mvn quarkus:dev
```

To generate initial data run `GraphGenerator.generate` - it will define a schema, randomly generate some data and write the data into CSV files that can be later used for load testing