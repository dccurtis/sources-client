import os
import logging

class Config:
    """Configuration for service."""
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    DB_NAME = os.getenv('DATABASE_NAME', 'sources-db')
    DB_USER = os.getenv('DATABASE_USER', 'postgres')
    DB_PASSWORD = os.getenv('DATABASE_PASSWORD', 'postgres')
    DB_HOST = os.getenv('POSTGRES_SQL_SERVICE_HOST', 'sources-db')
    DB_PORT = os.getenv('POSTGRES_SQL_SERVICE_PORT', '5432')
    DATABASE_URL = f'postgres://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    SOURCES_KAFKA_HOST = os.getenv('SOURCES_KAFKA_HOST', 'localhost')
    SOURCES_KAFKA_PORT = os.getenv('SOURCES_KAFKA_PORT', '29092')
    SOURCES_KAFKA_ADDRESS = f'{SOURCES_KAFKA_HOST}:{SOURCES_KAFKA_PORT}'
    SOURCES_TOPIC = os.getenv('SOURCES_KAFKA_TOPIC', 'platform.sources.event-stream')

    def setup_logger(self, logger):
        level = {'DEBUG': logging.DEBUG,
                 'INFO': logging.INFO,
                 'WARNING': logging.WARNING,
                 'ERROR': logging.ERROR}

        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(level.get(self.LOG_LEVEL))
        logger.addHandler(consoleHandler)
