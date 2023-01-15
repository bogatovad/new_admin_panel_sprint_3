from dotenv import dotenv_values

config = dotenv_values("enviroments/.env")

dsl = {
        'dbname': config.get('POSTGRES_DB'),
        'user': config.get('POSTGRES_USER'),
        'password': config.get('POSTGRES_PASSWORD'),
        'host': config.get('DB_HOST'),
        'port': int(config.get('DB_PORT')),
    }
