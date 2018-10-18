from .utils import TASK_KEYS, sha256
from ..ext.noconflict import noconflict
from .fields import DenormalizedList, DenormalizedFunction, DenormalizedMethod
from .exceptions import ModelFieldDeclarationError

from marshmallow.schema import Schema
import inflection
import warnings
import inspect


class ModelMeta(type):
    """Metaclass for the Model class. Provides a single registry of all subclasses for
    maintaining model database records. Registers subclasses to defined pipelines.
    """

    __registry = {}

    def __new__(mcs, name, bases, attrs):
        # check if a Denormalize field type exist on the class
        # this review needs to occur prior to the init as marshmallow
        # will consume all fields before returning the object's namespace
        denormalize = []
        for name_, attr in attrs.items():
            if isinstance(attr, (DenormalizedList, DenormalizedFunction, DenormalizedMethod)):
                denormalize.append(name_)

        if len(denormalize) > 1:
            # ensure only one Denormalize field type defined (cannot safely denormalize on multiple fields)
            raise ModelFieldDeclarationError(f'More than one denormalized field declared on model {name}: {denormalize}')
        elif denormalize:
            # store Denormalize field name for addition to the model
            attrs['_denormalize'] = denormalize[0]

        return super(ModelMeta, mcs).__new__(mcs, name, bases, attrs)

    def __init__(cls, name, bases, attrs):
        # fully-dotted path as object identity
        cls.__qname__ = f'{cls.__module__}.{cls.__name__}'

        if not hasattr(cls, '_registry'):
            # create reference to single registry across all subclasses
            cls._registry = ModelMeta.__registry

        else:
            # ensure registry entry exists for all subclasses (base class excluded)
            ModelMeta.__registry.setdefault(
                cls.__qname__,
                dict(cls=cls, version=None, pipeline=None,
                     pipeline_config=dict(
                         upload_accept=None, upload_role=None, upload_meta_schema=None,
                         **{f'has_{stage}': False for stage in TASK_KEYS},
                         **{f'{stage}_sha256': None for stage in TASK_KEYS}
                     ))
            )

            # default filename if none supplied
            if '__filename__' not in attrs:
                cls.__filename__ = inflection.tableize(name)

            # default parent directory if none supplied
            if '__directory__' not in attrs:
                cls.__directory__ = cls.__module__.split('.')[1]

            # store denormalize field name if present
            if '_denormalize' in attrs:
                cls._denormalize = True
                cls._denormalize_on = attrs['_denormalize']
            else:
                cls._denormalize = False

            # instantiate and register pipeline if defined
            if '__pipeline__' in attrs:
                cls._register_pipeline(attrs['__pipeline__'])

        super(ModelMeta, cls).__init__(name, bases, attrs)

    def _register_pipeline(cls, pipeline):
        """Register model with pipeline, build pipeline configuration data for model database entry."""
        p = pipeline(cls)
        config = dict(
            upload_accept=getattr(p, 'upload_accept', None),
            upload_role=getattr(p, 'upload_role', None),
            upload_meta_schema=getattr(p, 'upload_meta_schema', None)
        )
        config.update({f'has_{stage}': p.registry['task_schema'].get(stage, False) for stage in TASK_KEYS})
        config.update({f'{stage}_sha256': p.registry['task_hashes'].get(stage, None) for stage in TASK_KEYS})

        if config['has_transform']:
            # update pipeline transform hash with model hash (as the model defines transformation logic)
            # this occurs here so values are defined according to each model since a single pipeline may
            # serve multiple models
            config['transform_sha256'] = sha256(" ".join([config['transform_sha256'], inspect.getsource(cls)]))

        if config['has_load'] and getattr(cls, '__table__', False) is False:
            warnings.warn(f"Registered pipeline for model '{cls.__qname__}' implements the load stage "
                          f"but a table is not defined: set '__table__ = <SQL Alchemy Model>' on model to resolve, "
                          f"or set to 'None' to silence alert.")

        ModelMeta.__registry[cls.__qname__]['pipeline'] = p.__qname__
        ModelMeta.__registry[cls.__qname__]['pipeline_config'] = config


class Model(Schema, metaclass=noconflict(ModelMeta)):
    """Base model class with which to define data models."""

    def __init__(self, data, pkey=None, created=None, source=None, many=False, **kwargs):  # todo: test if many usage is possible
        super(Schema, self).__init__(many=many, **kwargs)
        self._data = data
        self._pkey = pkey
        self._created = created
        self._source = source

    def __repr__(self):
        try:
            return f"<{self.__qname__}(pipeline={self._registry[self.__qname__]['pipeline'].__qname__}, " \
                   f"version={self._registry[self.__qname__]['version']})>"

        except AttributeError:
            return f"<{self.__qname__}>"
