from ..ext.services import get_service
from ..data_pipes.exceptions import PipelineVersionError
from ..data_pipes.utils import db_cautious_interaction
from .models import DataModel
from datetime import datetime

db = get_service('db')
TASK_KEYS = ['upload', 'extract', 'transform', 'load']


class DataObject(db.Model):

    __tablename__ = '__etl_data_objects'

    pkey = db.Column(db.Integer, primary_key=True)
    created = db.Column(db.Date)
    model = db.Column(db.Unicode)               # fkey: __etl_data_models
    pipeline_version = db.Column(db.Integer)    # fkey: __etl_data_models

    uploaded = db.Column(db.Boolean, default=False)
    extracted = db.Column(db.Boolean, default=False)
    transformed = db.Column(db.Boolean, default=False)
    loaded = db.Column(db.Boolean, default=False)

    upload_date = db.Column(db.DateTime, nullable=True)
    extract_date = db.Column(db.DateTime, nullable=True)
    transform_date = db.Column(db.DateTime, nullable=True)
    load_date = db.Column(db.DateTime, nullable=True)

    upload_file = db.Column(db.Unicode, default=None)
    extract_file = db.Column(db.Unicode, default=None)
    transform_file = db.Column(db.Unicode, default=None)

    upload_user = db.Column(db.Integer, default=None)
    upload_meta = db.Column(db.JSON(none_as_null=True), default=None)

    pipeline_completed = db.Column(db.Boolean, default=False)

    def __init__(self, stage, file, model, created, user=None, meta=None):
        self.model = model
        self.created = created
        self.pipeline_version = self.get_version()
        self.set_etl_stage(stage)(file, date=created, user=user, meta=meta)

    def __iter__(self):
        return iter([self.uploaded, self.extracted, self.transformed, self.loaded])

    def __getitem__(self, item):
        return list(self)[item]

    def index(self, value):
        return list(self).index(value)

    def _next(self):
        start = self.index(True)
        # will raise ValueError if no remaining stages, error handled in outer `next` method
        idx = self[start:].index(False) + start
        return idx, TASK_KEYS[idx]

    @classmethod
    @db_cautious_interaction
    def upsert(cls, stage, file, pkey=None, created=None, model=None, user=None, meta=None):
        if pkey:
            entry = cls.query.get(pkey)
            entry.set_etl_stage(stage)(file)

        elif created and model:
            entry = cls(stage, model=model, file=file, created=created, user=user, meta=meta)
            db.session.add(entry)

        else:
            raise TypeError("upsert() missing required keyword argument: supply either 'pkey' or "
                            "'created' and 'model'")

        db.session.commit()

        # return meta dict
        return dict(pkey=entry.pkey, model=entry.model, file=file, created=entry.created)

    @classmethod
    def next(cls, pkey=None, entry=None):
        obj = cls.query.get(pkey) if pkey else entry

        try:
            idx, stage = obj._next()
            meta = dict(pkey=obj.pkey, model=obj.model, file=getattr(obj, f'{TASK_KEYS[idx - 1]}_file'),
                        created=obj.created)

            model = DataModel.query.filter_by(name=obj.model, pipeline_version=obj.pipeline_version).first()
            # validate that the next returned stage has been implemented for the given object
            if getattr(model, f'has_{stage}'):
                return stage, meta
            else:
                # object has been fully processed as it does not have the next
                # stage implemented for the given pipeline version
                raise ValueError

        except ValueError:
            # object has been fully processed
            return None, None

        except AttributeError:
            raise TypeError('next() requires at least 1 argument (0 given)')

    def advance(self, ignore_pipeline_version=False):
        from ..ext.services import get_service
        etl = get_service('etl')
        try:
            pipeline = etl.Model._registry[self.model]['pipeline']
            pipeline = etl.Pipeline._registry[pipeline]['self']

            if not ignore_pipeline_version:
                assert self.pipeline_version == self.get_version()

            pipeline.advance(instance=self)
        except (KeyError, AssertionError) as e:
            raise PipelineVersionError from e

    def get_version(self):
        model = DataModel.query.with_entities(DataModel.pipeline_version).filter_by(name=self.model).order_by(
            DataModel.pipeline_version.desc()).first()

        return model.pipeline_version

    def set_etl_stage(self, stage):
        _etl = {
            'upload': self._uploaded,
            'extract': self._extracted,
            'transform': self._transformed,
            'load': self._loaded
        }

        return _etl[stage]

    def _uploaded(self, file, user, date=None, meta=None):
        self.uploaded = True
        self.upload_date = date or datetime.utcnow()
        self.upload_file = file
        self.upload_user = user
        self.upload_meta = meta

    def _extracted(self, file, date=None, **kwargs):
        self.extracted = True
        self.extract_date = date or datetime.utcnow()
        self.extract_file = file

    def _transformed(self, file, **kwargs):
        self.transformed = True
        self.transform_date = datetime.utcnow()
        self.transform_file = file

    def _loaded(self, *args, **kwargs):
        self.loaded = True
        self.load_date = datetime.utcnow()

    def __repr__(self):
        return f"<DataObject(pkey='{self.pkey}', model='{self.model}')>"
