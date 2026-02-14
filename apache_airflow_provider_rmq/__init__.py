def get_provider_info():
    return {
        "package-name": "apache-airflow-provider-rmq",
        "name": "RabbitMQ Airflow Provider",
        "description": "RabbitMQ provider for Apache Airflow with publish, consume, sensor, and management operators.",
        "hook-class-names": ["apache_airflow_provider_rmq.hooks.rmq.RMQHook"],
        "connection-types": [
            {
                "hook-class-name": "apache_airflow_provider_rmq.hooks.rmq.RMQHook",
                "connection-type": "amqp",
            }
        ],
        "versions": ["1.0.0"],
    }