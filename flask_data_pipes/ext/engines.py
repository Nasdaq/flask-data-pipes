from sqlalchemy.exc import InvalidRequestError


class EngineRegistry(dict):

    def __init__(self, app, db):
        [super(EngineRegistry, self).__setitem__(db_name, EngineContainer(app=app, db=db, db_name=db_name)
                                                 ) for db_name in app.config['SQLALCHEMY_BINDS']]

    def __getattr__(self, item):
        try:
            return self.__getitem__(item)
        except KeyError:
            raise InvalidRequestError(f" [EngineRegistryUsage] SQLAlchemy Bind does not exist: '{item}'")


class EngineContainer(object):

    def __init__(self, app, db, db_name):
        self.engine = db.get_engine(app=app, bind=db_name)
        self.meta = db.MetaData(bind=self.engine)

    def __getattr__(self, item):
        return self.engine.__getattribute__(item)
