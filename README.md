# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL. Подробности задания в папке `etl`.

# Запуск контейнеров
Скопировать структуру содержимого .env.example в .env перед запуском со своими данными
```bash
cp .env.example .env
```

Запуск контейнеров
```bash
docker-compose down && docker-compose build && docker-compose up -d
```

Доступ к psql в postgres внутри контейнера.
``` bash
docker exec -it etl_postgres_1 psql -h <HOST> -d <NAME> -U <USER>
```

Доступ к контейнеру etl.
``` bash
docker exec -it etl_etl_1 /bin/bash
```