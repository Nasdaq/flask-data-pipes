def get_service(name):
    from flask import current_app as app
    if name is 'db':
        return app.extensions['sqlalchemy'].db
    elif name is 'etl':
        return app.extensions['data_pipes']
    else:
        return DisabledService()


class DisabledService:
    def __getattr__(self, item):
        return self

    def __call__(self, *args, **kwargs):
        pass


