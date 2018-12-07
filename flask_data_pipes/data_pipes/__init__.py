from .model import Model
from .pipeline import Pipeline
from . import fields

from .utils import sha256
from .utils import strip_trailing_slash, AttrDict

from datetime import datetime
from werkzeug.utils import secure_filename
from flask import current_app, request, g, _app_ctx_stack
from pathlib import Path
from enum import Enum
import warnings
import os

FLASK_EXTENSION_NAME = 'data_pipes'


class DefaultFileType(Enum):
    data = ['csv', 'json']
    image = ['png', 'jpg']
    document = ['txt']
    archive = ['zip']
    csv = ['csv']
    json = ['json']
    zip = ['zip']


class ETL(object):

    def __init__(self, app=None, db=None):
        self.Model = Model
        self.Pipeline = Pipeline
        self.fields = fields
        self.filetype = DefaultFileType
        self._DataModel = None
        self._DataObject = None

        self.app = app
        self.db = db
        if app is not None and db is not None:
            self.init_app(app, db)

    def init_app(self, app, db):
        """Initializes the application with the extension.

        :param Flask app: The Flask application object.
        :param db: The sqlalchemy db object.
        """

        if not hasattr(app, 'celery'):
            raise NotImplementedError(
                "Celery object expected as 'celery' attribute on Flask app object. "
                "Make sure Celery is initialized prior to this extension."
            )

        if 'DATA' not in app.config:
            warnings.warn('DATA directory not set. Defaulting to "/tmp/data".')

        if 'DATA_FORMAT' not in app.config:
            warnings.warn('DATA_FORMAT not set. Defaulting to "json_lines".')

        if 'DATA_COMPRESSION' not in app.config:
            warnings.warn('DATA_COMPRESSION not set. Defaulting to "True".')

        if 'DATA_ENCODING' not in app.config:
            warnings.warn('DATA_ENCODING not set. Defaulting to "utf-8".')

        if 'DATA_ENCODING_ERRORS' not in app.config:
            warnings.warn('DATA_ENCODING not set. Defaulting to "surrogateescape".')

        if 'ACCEPT' not in app.config:
            warnings.warn('ACCEPT not set. Default upload accept values will be used.')

        # set data directory defaults if not assigned within app config
        app.config.setdefault('DATA', '/tmp/data')
        app.config.setdefault('DATA_TEMP', os.path.join(app.config['DATA'], 'tmp'))
        app.config.setdefault('DATA_UPLOAD', os.path.join(app.config['DATA'], 'uploads'))
        app.config.setdefault('DATA_EXTRACT', os.path.join(app.config['DATA'], 'raw'))
        app.config.setdefault('DATA_TRANSFORM', os.path.join(app.config['DATA'], 'transformed'))

        # create data directories if not present
        [os.mkdir(dir_) for dir_ in [app.config['DATA'],
                                     app.config['DATA_TEMP'],
                                     app.config['DATA_UPLOAD'],
                                     app.config['DATA_EXTRACT'],
                                     app.config['DATA_TRANSFORM']
                                     ] if not os.path.exists(dir_)]

        # place lock file in temp directory to prevent deletion during file close operation
        Path(app.config['DATA_TEMP'], '.dirlock').touch()

        # set configuration defaults if not assigned within app config
        app.config.setdefault('DATA_FORMAT', 'json_lines')
        app.config.setdefault('DATA_COMPRESSION', True)
        app.config.setdefault('DATA_ENCODING', 'utf-8')
        app.config.setdefault('DATA_ENCODING_ERRORS', 'surrogateescape')
        app.config.setdefault('ETL_SAVE_ON_TEARDOWN', False)

        app.config.setdefault('ACCEPT', {})
        app.config['ACCEPT'].setdefault('DATA', ['csv', 'json'])
        app.config['ACCEPT'].setdefault('IMAGE', ['png', 'jpg'])
        app.config['ACCEPT'].setdefault('DOCUMENT', ['txt'])
        app.config['ACCEPT'].setdefault('ARCHIVE', ['zip'])
        app.config['ACCEPT'].setdefault('CSV', ['csv'])
        app.config['ACCEPT'].setdefault('JSON', ['json'])
        app.config['ACCEPT'].setdefault('ZIP', ['zip'])

        app.config['ACCEPT']['ANY'] = set()
        for accepted in app.config['ACCEPT'].values():
            [app.config['ACCEPT']['ANY'].add(ext) for ext in accepted]

        self.Pipeline.config.update({
            'DATA': strip_trailing_slash(app.config['DATA']),
            'DATA_TEMP': strip_trailing_slash(app.config['DATA_TEMP']),
            'DATA_UPLOAD': strip_trailing_slash(app.config['DATA_UPLOAD']),
            'DATA_EXTRACT': strip_trailing_slash(app.config['DATA_EXTRACT']),
            'DATA_TRANSFORM': strip_trailing_slash(app.config['DATA_TRANSFORM']),
            'DATA_FORMAT': app.config['DATA_FORMAT'],
            'DATA_COMPRESSION': app.config['DATA_COMPRESSION'],
            'DATA_ENCODING': app.config['DATA_ENCODING'],
            'DATA_ENCODING_ERRORS': app.config['DATA_ENCODING_ERRORS']
        })

        with app.app_context():
            from .filetypes import FileType
            self.filetype = FileType

            from .tables import table_factory
            self._DataModel, self._DataObject = table_factory(db, self.filetype)

            # add all tasks into namespace
            # add tasks to task exec registry (AttrDict)
            from .tasks import pipeline_task, processor_task, restart_stalled_pipelines
            self.Pipeline.exec.pipeline = pipeline_task
            self.Pipeline.exec.processor = processor_task

            # add DataObject next and insert-or-update method, db session and engine to db registry (AttrDict)
            self.Pipeline.db.next = self._DataObject.next
            self.Pipeline.db.upsert = self._DataObject.upsert
            self.Pipeline.db.session = db.session

        # hack to stash engine property without executing get_engine(), necessary to avoid race condition on start up
        class Engine:

            @property
            def engine(self):
                return db.get_engine()

            def __getattr__(self, item):
                return getattr(self.engine, item)

        self.Pipeline.db.engine = Engine()

        if getattr(app, 'signal', None):
            app.signal.register('etl_tables_imported', subscriber=self.push_registered_models)

        else:
            warnings.warn('Signal system not initialized on app. Tasks will not be registered.')

        app.extensions = getattr(app, 'extensions', {})
        app.extensions[FLASK_EXTENSION_NAME] = self

        @app.teardown_appcontext
        def close_all_files(exception):
            ctx = _app_ctx_stack.top
            if ctx and hasattr(ctx, 'etl_pipeline_session'):
                for file in ctx.etl_pipeline_session.file.values():
                    file.close()

                if not ctx.app.config['ETL_SAVE_ON_TEARDOWN']:
                    for path in ctx.etl_pipeline_session.write.values():
                        os.remove(path)

    def get_app(self, reference_app=None):
        """Helper method that implements the logic to look up an
        application."""

        if reference_app is not None:
            return reference_app

        if current_app:
            return current_app._get_current_object()

        if self.app is not None:
            return self.app

        raise RuntimeError(
            "No application found. Either work inside a view function or push an application context."
        )

    def push_registered_models(self, sender):
        """Updates db with registered tasks. Sender is the app object."""
        db = sender.extensions['sqlalchemy'].db
        for model, registry in self.Model._registry.items():
            etl = self._DataModel(
                name=model,
                pipeline=registry['pipeline'],
                directory=registry['cls'].__directory__,
                filename=registry['cls'].__filename__,
                **registry['pipeline_config']
            )
            db.session.merge(etl)

        db.session.commit()
        db.session.remove()
        sender.logger.info('Registered etl stages updated.')

    def upload(self, func):
        """
        Decorator function for file upload resources. The decorated
        function gets evaluated immediately, allowing the request object
        to be modified prior to validation and pipeline execution.


        Validates: file exists, model name, file type accepted,
        upload active and user permissions. Executes registered
        pipeline to process uploaded data.

        :type func: post method
        """
        from ..ext.roles import require_role

        try:
            logger = self.get_app().logger
        except AttributeError:
            from ..ext.services import DisabledService
            logger = DisabledService()

        def wrapper(*args, **kwargs):
            timestamp = datetime.utcnow()
            func(*args, **kwargs)
            form = request.form.to_dict()

            try:

                modelname = form.pop('data_model')
                model = self._DataModel.query.filter_by(name=modelname).order_by(
                    self._DataModel.pipeline_version.desc()).first_or_404()

                pipeline = self.Pipeline._registry[model.pipeline]['self']

                for key, file in request.files.items():

                    ext = file.filename.rsplit('.', 1)[1].lower()

                    if ext in model.upload_accept.value and model.upload_active:
                        # todo: future: validate meta
                        filename = sha256(timestamp.isoformat() + file.filename.lower())[:8] + '_' + secure_filename(
                            file.filename.lower())

                        logger.info(f"File upload accepted from '{g.user.username}' for '{modelname}': {filename}")

                        require_role(model.upload_role)(
                            pipeline(
                                meta=dict(file=file, filename=filename, model=model.name, created=timestamp, user=g.user.id, **form),
                                stage='upload'
                            ))

                        # todo: issue 303 to GET status of uploaded file

                    else:
                        logger.warn(f"Invalid file upload operation attempted by '{g.user.username}': "
                                    f"{file.filename} -> {modelname}")
                        return 415

            except (KeyError, IndexError):
                # no file uploaded or invalid form values
                # return redirect(request.url), 400
                logger.warn(f"Malformed file upload request submitted by '{g.user.username}'")
                return 400
            # wait for signal
            return 201

        return wrapper
