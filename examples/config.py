import os
from urllib.parse import quote_plus as urlquote

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DEBUG = False
    USE_SIGNALS = True

    BASE_DIR = basedir
    DATA = os.getenv('DATA_DIR', os.path.join(BASE_DIR, 'appdata'))

    DATA_FORMAT = 'json_lines'
    DATA_COMPRESSION = False
    DATA_ENCODING = 'utf-8'
    DATA_ENCODING_ERRORS = 'surrogateescape'

    SQLALCHEMY_DATABASE_URI = f'sqlite:///{DATA}/demo.db'
    CELERY_BROKER_URI = 'amqp://guest@localhost//'

    MODULES = []

    @classmethod
    def init_app(cls, app):
        # config modules
        app.config['MODULES'].extend(['auth', 'celery', 'utils'])
        app.config['MODULES'] = sorted(set(app.config['MODULES']))

        # config sqlalchemy bind
        app.config['SQLALCHEMY_BINDS'] = cls.config_sqlalchemy_binds(app)

    @staticmethod
    def config_sqlalchemy_binds(app):
        db_connections = {}
        for mod in app.config['MODULES']:
            try:
                db_connections.update(
                    {mod.lower(): app.config[mod.upper()]['dsn'].format(user=urlquote(app.config[mod.upper()]['user']),
                                                                        pwd=urlquote(app.config[mod.upper()]['pwd']))})
            except AttributeError:
                for name, cnxn in app.config[mod.upper()]['dsn'].items():
                    try:
                        db_connections.update({name.lower(): cnxn['dsn'].format(user=urlquote(cnxn['user']),
                                                                                pwd=urlquote(cnxn['pwd']))})
                    except KeyError:
                        pass

            except KeyError:
                pass

        return db_connections


class DevelopmentConfig(Config):
    ENV = 'DEV'
    DEBUG = True
    SQLALCHEMY_RECORD_QUERIES = True
    SQLALCHEMY_ECHO = True
    SQLALCHEMY_BINDS = {}

    ETL_SAVE_ON_TEARDOWN = True

    @classmethod
    def init_app(cls, app):
        app.config.from_json(os.path.join(basedir, 'config.json'))
        Config.init_app(app)


class ProductionConfig(Config):
    ENV = 'PROD'
    DEBUG = False
    DATA_COMPRESSION = True
    SQLALCHEMY_RECORD_QUERIES = False
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_BINDS = {}

    @classmethod
    def init_app(cls, app):
        app.config.from_json(os.path.join(basedir, 'config.json'))
        Config.init_app(app)


config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}
