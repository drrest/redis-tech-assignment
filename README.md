# REDIS TECHNICAL ASSIGNMENT

## Introduction
Complex solution to cover the requirements of the technical assignment.

## Requirements
docker + docker-compose

## What to setup?
**docker-compose.yml** file contains all the services needed to run the solution.
You can easely configure parameter in docker-compose.yml file to run consumers for example with 
changed amount of consumers

## How to run
```bash
docker-compose up -d
```

## How to produce data
```bash
 docker compose exec monitoring python src/publisher.py
```

## How to see logs of the services
```bash
docker-compose logs -f --tail=1000
```

## How to test
```bash
docker compose exec monitoring python -m unittest discover -s tests
```

# How to discover data
RedisInsight is added to the solution so you can easely see the data

Open your browser and go to http://127.0.0.1:5540
instance of redis is added early so you can just click 
on redis:6379 and see the data
