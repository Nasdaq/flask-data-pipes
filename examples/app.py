from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_data_pipes import ETL, EngineRegistry, Signal
from celery import Celery
from importlib import import_module
from config import config
import os

BASE = os.path.dirname(__file__)

db = SQLAlchemy()
etl = ETL()


def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    db.init_app(app)

    app.signal = Signal()
    app.engine = EngineRegistry(app=app, db=db)
    app.celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URI'])

    return app


def import_models(app):
    with app.app_context():
        for module_name in app.config['MODULES']:
            try:
                import_module(f'{BASE}.{module_name}.models', package=__name__)
            except (AttributeError, ModuleNotFoundError):
                continue

        app.signal.models_imported.send(app)


def import_tasks(app):
    with app.app_context():
        for module_name in app.config['MODULES']:
            try:
                import_module(f'{BASE}.{module_name}.tasks', package=__name__)
                app.logger.info(f'Task module imported: {module_name}')
            except ModuleNotFoundError:
                app.logger.warn(f"Task module not imported: {BASE}.{module_name}.tasks not found.")
                continue


app = create_app(os.getenv('APPENV', 'default'))
etl.init_app(app, db)
import_models(app)

if __name__ == '__main__':
    app.run(port=8080, threaded=True)
