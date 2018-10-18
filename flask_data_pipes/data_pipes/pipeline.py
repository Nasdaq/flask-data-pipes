from .utils import TASK_KEYS, sha256, AttrDict, denormalize, pass_through
from .decorators import __SYNC_TAGS__, __ASYNC_TAGS__
from ..ext import serializers
from .exceptions import (PipelineError, PipelineModelError,
                         PipelineTaskSchemaError, PipelineExecutionError,
                         PipelineDataError, StopPipeline)

from collections import defaultdict
from typing import Generator, List, Tuple
from datetime import datetime
from flask import _app_ctx_stack
from celery import chain
from enum import Enum
from itertools import islice
import functools
import inspect
import warnings
import gzip
import json
import csv
import os


class PipelineMeta(type):
    """Metaclass for the Pipeline class. Provides a single registry
     of all subclasses for maintaining pipeline execution data.
     """

    # these registry attributes need to be mutable,
    # else they are not referenced properly across subclasses
    __registry = {}
    __config = {}

    # while this should be moved to the app context, a scopedsession
    # was added to the app context stack to avoid potential issues
    __session = AttrDict(read={}, write={}, file={})
    __exec = AttrDict(pipeline=None, processor=None)
    __db = AttrDict(next=None, upsert=None, session=None)

    def __new__(mcs, name, bases, attrs, upload=False, extract=False, transform=False, load=False):

        # enforce configuration value types (encoding configs excluded due to matching properties and laziness)
        if 'upload_accept' in attrs and not isinstance(attrs['upload_accept'], Enum):
            raise ValueError("`upload_accept` option must be an Enum: use io.filetype")

        if 'upload_role' in attrs and not isinstance(attrs['upload_role'], Enum):
            raise ValueError("`upload_role` option must be an Enum: use auth.utils.auth_helper.Role")

        if 'upload_meta_schema' in attrs and not isinstance(attrs['upload_meta_schema'], dict):
            raise ValueError("`upload_meta_schema` option must be a dict")

        if 'autocommit' in attrs and not isinstance(attrs['autocommit'], bool):
            raise ValueError("`autocommit` option must be a bool")

        return super(PipelineMeta, mcs).__new__(mcs, name, bases, attrs)

    def __init__(cls, name, bases, attrs, **kwargs):
        # fully-dotted path as object identity
        cls.__qname__ = f'{cls.__module__}.{cls.__name__}'

        # create reference to single registries across all subclasses
        # note: cls.__registry != PipelineMeta.__registry due to name mangling: class attr `__foo` becomes `_Class__foo`
        cls._registry = PipelineMeta.__registry     # primary registry
        cls.config = PipelineMeta.__config          # config registry (avoid need for importing app)
        cls.exec = PipelineMeta.__exec              # task registry
        cls.db = PipelineMeta.__db                  # ref to db registry to avoid import race conditions
        cls.session = PipelineMeta.__session        # read and write session, keyed by (model.__qname__, pkey)

        # mapper index from user-defined names to registry keys,
        # see ``models`` method for example usage.
        cls._index = {}

        # set json for dumps
        # todo: build based on config
        cls.output = functools.partial(json.dumps, default=serializers.serialize, ensure_ascii=False, skipkeys=True)

        if bases:
            # ensure registry entry exists for all subclasses (base class excluded)
            # entry for ``self`` included for singleton implementation in base class
            PipelineMeta.__registry.setdefault(
                cls.__qname__,
                dict(cls=cls, self=None, models={}, task_schema=None, task_hashes=None, pipes=None,
                     processors=dict(sync={tag: [] for tag in __SYNC_TAGS__}, async={tag: [] for tag in __ASYNC_TAGS__}),
                     stages={stage: None for stage in TASK_KEYS},
                     params={stage: None for stage in TASK_KEYS},
                     encoding={'type': attrs.get('encoding', None), 'errors': attrs.get('encoding_errors', None)},
                     compression=attrs.get('compression', None),
                     autocommit=attrs.get('autocommit', True))
            )

            cls._register_pipeline_schema(**kwargs)
            cls._register_processors()
            cls._register_tasks()

        super(PipelineMeta, cls).__init__(name, bases, attrs)

    def _register_pipeline_schema(cls, **kwargs):
        """Register task schema from subclass declaration.

        :raises PipelineTaskSchemaError
        """
        tasks = {key: kwargs.pop(key, False) for key in TASK_KEYS}

        has_tasks = False
        for state in tasks.values():
            has_tasks = state or has_tasks

        # raise PipelineTaskSchemaError if task declaration is nonconsecutive
        if tasks['load'] and not tasks['transform'] and not tasks['extract'] and not tasks['upload']:
            pass
        elif (tasks['load'] and not tasks['transform']) \
                or (tasks['transform'] and not tasks['extract']):

            raise PipelineTaskSchemaError(f'Invalid task schema declared on class {cls.__name__}: {tasks}')

        # warn if no tasks declared
        elif not has_tasks:
            warnings.warn(f'Task schema not set on class {cls.__name__}, no pipeline tasks will be executed.')

        # add schema to object and registry
        PipelineMeta.__registry[cls.__qname__]['task_schema'] = cls.schema = tasks

    def _register_processors(cls):
        """Register decorated processor and core stage functions"""
        mro = inspect.getmro(cls)
        processor_kwargs = {stage: {} for stage in TASK_KEYS}
        stages = {stage: [] for stage in TASK_KEYS}
        is_async = {True: 'async', False: 'sync'}
        for attr_name in dir(cls):
            # Need to look up the actual descriptor, not whatever might be
            # bound to the class. This needs to come from the __dict__ of the
            # declaring class.
            for parent in mro:
                try:
                    attr = parent.__dict__[attr_name]
                except KeyError:
                    continue
                else:
                    break
            else:

                continue

            try:
                processor_tags = attr.__pipeline_tags__
            except AttributeError:
                continue

            for tag in processor_tags:
                # Use name here so we can get the bound method later, in case
                # the processor was a descriptor or something.
                # tag is keyed as (<tag_name>, <async>)
                if tag[0] in TASK_KEYS:
                    # append core stage functions, stash kwargs
                    stages[tag[0]].append(attr_name)
                    processor_kwargs[tag[0]] = attr.__pipeline_kwargs__[tag]
                else:
                    # Store in registry as {'sync': {<tag_name>:[], ... },  'async': {<tag_name>:[], ... }}
                    PipelineMeta.__registry[cls.__qname__]['processors'][is_async[tag[1]]][tag[0]].append(attr_name)

        for stage, collection in stages.items():
            # set default many key for each stage
            processor_kwargs[stage].setdefault('many', False if stage != 'load' else True)

            # check the method count for each stage (core only, not pre/post methods)
            # raise PipelineError if more than one core method defined per stage
            # store user-defined method or default in registry as string with defined kwargs
            if len(collection) == 1:
                PipelineMeta.__registry[cls.__qname__]['stages'][stage] = (collection[0], processor_kwargs[stage])
            elif len(collection) == 0:
                PipelineMeta.__registry[cls.__qname__]['stages'][stage] = (f'_{stage}', processor_kwargs[stage])
            else:
                raise PipelineError(f'Multiple `{stage}` methods defined, only one permitted. '
                                    f'Use processors to extend {stage} functionality.')

    def _register_tasks(cls):
        """Hash the list of functions for each active stage for pipeline versioning."""
        tasks = defaultdict(list)
        for stage, active in PipelineMeta.__registry[cls.__qname__]['task_schema'].items():
            # note: this is not building the execution order for each stage (see _build_pipes)
            # this method builds a list of methods simply for hashing
            if active:
                # add user-defined pre_processors
                # note: async processors are excluded as they do not have
                # the ability to modify the stream of data
                tasks[stage].extend(
                    PipelineMeta.__registry[cls.__qname__]['processors']['sync'][f'pre_{stage}']
                )
                # add core and primary execution methods
                tasks[stage].extend([
                    PipelineMeta.__registry[cls.__qname__]['stages'][stage][0],
                    f'_{stage}_processor'

                ])

                if stage not in ['upload', 'load']:
                    # add user-defined post_processor (not available for `upload` or `load`)
                    tasks[stage].extend(
                        PipelineMeta.__registry[cls.__qname__]['processors']['sync'][f'post_{stage}']
                    )

            else:
                continue

        task_hashes = defaultdict(list)
        for stage, func_list in tasks.items():
            for func in func_list:
                # append source code for each function in the pipe
                task_hashes[stage].append(inspect.getsource(getattr(cls, func)))

            # append core execution method kwargs
            task_hashes[stage].append(str(PipelineMeta.__registry[cls.__qname__]['stages'][stage][1]))

            # hash the source of each function in the pipeline for versioning
            task_hashes[stage] = sha256(" ".join(task_hashes[stage]))

        # add hashes to registry (converting dict list back into dict)
        # note: for the transform stage, the hash value registered here will
        # be rehashed with a hash of the io model (as the model defines transformation
        # logic) prior to updating the db record for each model.
        PipelineMeta.__registry[cls.__qname__]['task_hashes'] = dict(task_hashes)


class Pipeline(metaclass=PipelineMeta):

    def __new__(cls, *args, **kwargs):
        # singleton implementation: ensure a single instance of each pipeline subclass is created
        if not cls._registry[cls.__qname__]['self']:
            cls._registry[cls.__qname__]['self'] = self = super(Pipeline, cls).__new__(cls)

            # build pipes and collect processors here to ensure we get the bound methods,
            # passing in singleton instance
            cls._build_pipes(self)
            cls._collect_stages(self)
            cls._collect_processors(self)

        return cls._registry[cls.__qname__]['self']

    def __init__(self, model=None, stage=None, meta=None):
        # instantiation only available for registering models
        # else, run pipeline (see singleton implementation above)
        if model:
            self._register_model(model)
        else:
            self(stage, meta)

    def __call__(self, stage=None, meta=None):
        if stage is None:
            _pipeline = chain(*(self.pipes[task] for task in TASK_KEYS if self.schema[task]))
            _pipeline.delay(meta)
        elif stage is not 'upload':
            _pipeline = chain(*(self.pipes[task] for task in TASK_KEYS[TASK_KEYS.index(stage):] if self.schema[task]))
            _pipeline.delay(meta)
        else:
            _pipeline = chain(*(self.pipes[task] for task in TASK_KEYS[TASK_KEYS.index('extract'):] if self.schema[task]))
            _pipeline.delay(self._upload_processor(**meta))
    
    @staticmethod
    def _build_pipes(self):
        """Build pipeline tasks using celery task signature."""
        pipes = defaultdict(list)

        for stage, active in self.schema.items():
            if active:
                # add primary executor for each stage
                pipes[stage].append(getattr(self, f'_{stage}_processor'))

                if stage in ['extract', 'transform']:
                    # extract and transform require a writer method
                    # writer method is seeded with the stage name and autocommit setting via a partial function
                    pipes[stage].append(
                        functools.partial(getattr(self, '_writer'), stage=stage, autocommit=self.registry['autocommit'])
                    )

                # create a partial task signature for each pipe (passing in name and funcs)
                pipes[stage] = self.exec.pipeline.signature((f'{self.__qname__}: {stage}', pipes[stage]))

        # add pipes to registry (converting dict list back into dict)
        self.registry['pipes'] = self.pipes = dict(pipes)

    @staticmethod
    def _collect_processors(self):
        """Collect user-decorated processor methods."""
        for synchrony, collection in self.registry['processors'].items():
            for name, funcs in collection.items():
                for idx, func in enumerate(funcs):
                    # split processor dict between sync and async
                    # iterate over k, v -> tag name, list of function descriptors
                    # enumerate the list of functions
                    # replace method descriptor with bound method, maintaining order
                    self.registry['processors'][synchrony][name][idx] = getattr(self, func)  # todo: if processor has kwargs, build partial (determine if kwargs have a use case first)

    @staticmethod
    def _collect_stages(self):
        """Collect core stage methods."""

        def exec_single(func):

            def wrapper(databundle: List[Tuple[Generator, dict]]):
                for generator, meta in databundle:
                    for data in generator:
                        yield func(data, meta)

            return wrapper

        # def exec_many(func):
        #
        #     def wrapper(databundle: list):
        #         metas = []
        #         data = []
        #         for generator, meta in databundle:
        #             metas.append(meta)
        #             data.append([entry for entry in generator])
        #
        #         return func(data, metas)
        #
        #     return wrapper

        for stage, collection in self.registry['stages'].items():
            stage_kwargs = collection[1]
            many = stage_kwargs.get('many', False if stage != 'load' else True)

            # wrap core function in single execute function if not many or upload and not extract without upload
            if stage == 'extract' and not self.schema['upload']:
                self.registry['stages'][stage] = getattr(self, collection[0])
            elif not many and stage != 'upload':
                self.registry['stages'][stage] = exec_single(getattr(self, collection[0]))
            else:
                self.registry['stages'][stage] = getattr(self, collection[0])

            # add stage params to registry
            self.registry['params'][stage] = stage_kwargs

    def _register_model(self, cls):
        self.registry['models'].setdefault(cls.__qname__, cls)

    @property
    def scopedsession(self):
        """File sessions managed on the app context."""
        ctx = _app_ctx_stack.top
        if ctx:
            if not hasattr(ctx, 'etl_pipeline_session'):
                ctx.etl_pipeline_session = AttrDict(read={}, write={}, file={})

        return getattr(ctx, 'etl_pipeline_session', AttrDict(read={}, write={}, file={}))

    @property
    def registry(self):
        return self._registry[self.__qname__]

    @property
    def compression(self):
        return self.registry['compression'] or self.config['DATA_COMPRESSION']

    @property
    def encoding(self):
        return self.registry['encoding']['type'] or self.config['DATA_ENCODING']

    @property
    def encoding_errors(self):
        return self.registry['encoding']['errors'] or self.config['DATA_ENCODING_ERRORS']

    def models(self, name: str, data=None, pkey=None, created=None, source=None):
        """Pass data to a model using its string name (class name,
        partial or fully-dotted path). Used to avoid circular imports
        and enable model abstraction in downstream tasks.

        Usage:
            ```
            @extract
            def extract(self, *args, **kwargs):
                with self.client as c:
                    yield self.models('Computer', c.get_computers())
                    yield self.models('symantec.Group' c.get_groups())
            ```

        :param name: str, name of model class (can be partial for fully-dotted to avoid ambiguity)
        :param data: dict, data to be passed to the returned model via **kwargs
        :param pkey: int, DataObject primary key
        :param created: datetime, DataObject creation timestamp
        :param source: str, path of the file from which the data originated

        :raises PipelineModelError: if no or multiple models found for the given name.
        :returns: instance of referenced model, initialized with provided data.
        """

        try:
            keys = [self._index[name]]

        except KeyError:
            models = list(self.registry['models'].keys())
            search = name.split('.')
            search.reverse()

            keys = []
            for model in models:
                ref = model.split('.')
                ref.reverse()
                if ref[:len(search)] == search:
                    keys.append(model)

            if len(keys) == 0:
                raise PipelineModelError(f"Class with name '{name}' was not found. "
                                         f"You may need to register the pipeline "
                                         f"with the model.")
            elif len(keys) > 1:
                raise PipelineModelError(f"Multiple classes with name '{name}' "
                                         "were found. Please use the full, "
                                         "module-qualified path.")

            else:
                self._index[name] = keys[0]

        cls = self.registry['models'][keys[0]]
        if data or pkey:
            return cls(data=data, pkey=pkey, created=created, source=source)
        else:
            return cls

    def tables(self, name: str, data=None):
        """Returns the db table or an instance of the table registered
        to a given io model."""
        try:
            if data:
                return self.models(name).__table__(**data)
            else:
                return self.models(name).__table__
        except AttributeError:
            raise PipelineModelError(f"Table not defined on model '{name}': "
                                     f"set '__table__ = <SQL Alchemy Model>' on class to resolve.")

    def advance(self, pkey: int=None, instance: object=None):
        """Class method for advancing the pipeline execution of a DataObject.

        Usage:
            ```
            from hub.tenable.tasks import TenablePipeline
            TenablePipeline.advance(pkey=11)
            ```

        :param pkey: int, primary key of a DataObject entry
        :param instance: DataObject instance

        :raises PipelineExecutionError: if DataObject not associated with
        the pipeline or if the DataObject does not any unprocessed stages.
        """
        if not (pkey or instance):
            raise PipelineExecutionError("DataObject pkey or instance must be provided to advance the pipeline.")

        stage, meta = self.db.next(pkey=pkey, entry=instance)

        if not stage:
            raise StopPipeline(f"DataObject(pkey={pkey} has no remaining stages: "
                               f"object appears to be fully processed.")

        elif meta['model'] not in self._registry[self.__qname__]['models']:
            raise PipelineExecutionError(f"Model '{meta['model']}' not registered to this "
                                         f"pipeline: check provided pkey {pkey}")

        else:
            self(stage=stage, meta=[meta])

    def _upload(self, file, filename: str, model: str, created: datetime, **kwargs) -> str:
        path = os.path.join(self.config['DATA_UPLOAD'],
                            self.models(model).__directory__,
                            created.strftime('%Y'),
                            created.strftime('%m'),
                            created.strftime('%d'),
                            filename)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        file.save(path)
        return path

    def _upload_processor(self, file, filename: str, model: str, user: str, created: datetime, **kwargs) -> list:
        # compile args and kwargs
        attrs = dict(filename=filename, model=model, created=created, **kwargs)

        # execute pre upload processors (async)
        for processor in self.registry['processors']['async']['pre_upload']:
            self.exec.processor.delay(name=f'{self.__qname__}.pre_upload: {processor.__name__}',
                                      func=processor,
                                      **attrs)

        # execute pre upload processors (sync) passing in meta
        for processor in self.registry['processors']['sync']['pre_upload']:
            attrs = processor(**attrs)

        # call core upload method: save file to disk
        path = self.registry['stages']['upload'](file, **attrs)

        # create db record for uploaded data object
        meta = self.db.upsert(stage='upload', file=path, created=created, model=model, user=user, meta=kwargs.get('meta', None))

        # execute on upload commit processors (async)
        for processor in self.registry['processors']['async']['on_upload_commit']:
            self.exec.processor.delay(name=f'{self.__qname__}.on_upload_commit: {processor.__name__}',
                                      func=processor, **dict(meta=[meta]))

        return [meta]

    # todo: document if many=True, must return generator
    def _extract(self, data: dict, meta: dict):
        return self.models(meta['model'], data, pkey=meta['pkey'], created=meta['created'], source=meta['file'])

    def _extract_processor(self, meta: List[dict]=None) -> Generator:
        # execute pre extract processors (async)
        for processor in self.registry['processors']['async']['pre_extract']:
            self.exec.processor.delay(name=f'{self.__qname__}.pre_extract: {processor.__name__}',
                                      func=processor, **dict(meta=meta))

        # execute pre extract processors (sync) passing in meta
        for processor in self.registry['processors']['sync']['pre_extract']:
            meta = processor(meta=meta)

        # build data reader generator list if upload exists
        # in task schema (no file to read if this is the first stage)
        if self.schema['upload']:
            data = []
            for entry in meta:
                data.append((self._reader(entry, stage='extract'), entry))
        else:
            data = meta

        # execute extract generator, evaluated many true/false based on registered stage function
        for o in self.registry['stages']['extract'](data):

            try:
                # grab the raw data (o._data: dict)
                data = o._data
            except AttributeError:
                raise PipelineExecutionError(f'Extract method must yield an instance of io.Model, use '
                                             f'self.models method to resolve: received {type(o)}')

            # execute post extract processors (sync) passing in data and the rebuilt meta
            for processor in self.registry['processors']['sync']['post_extract']:
                data = processor(data=data,
                                 meta=dict(pkey=o._pkey, model=o.__qname__, file=o._source, created=o._created))
                # potential future option: pass many is true; but would slow down pipeline

            if data:
                # confirm data is not null to prevent writing blank lines to file
                # this provides the ability to implement post processors which
                # filter the data based on some user-defined criteria

                # o is an instance of the specified model (an io.Model)
                o._data = data

                yield o
            else:
                continue

    def _transform(self, data: dict, meta: dict):
        return self.models(meta['model'], data, pkey=meta['pkey'], created=meta['created'], source=meta['file'])

    def _transform_processor(self, meta: List[dict]) -> Generator:
        # execute pre transform processors (async)
        for processor in self.registry['processors']['async']['pre_transform']:
            self.exec.processor.delay(name=f'{self.__qname__}.pre_transform: {processor.__name__}',
                                      func=processor, **dict(meta=meta))

        # execute pre transform processors (sync) passing in meta
        for processor in self.registry['processors']['sync']['pre_transform']:
            meta = processor(meta=meta)

        data = []
        for entry in meta:
            # build data reader generator list
            data.append((self._reader(entry, stage='transform'), entry))

        # execute transform generator, evaluated many true/false based on registered stage function
        for o in self.registry['stages']['transform'](data):

            try:
                # use the marshmallow schema to serialize the data (using dump
                # not dumps so dict can be passed to any post processors)
                data = o.dump(o._data)
            except AttributeError:
                raise PipelineExecutionError(f'Transform method must yield an instance of io.Model, use '
                                             f'self.models method to resolve: received {type(o)}')

            if o._denormalize:
                # denormalize data if a Denormalize field declared
                data = denormalize(data, o._denormalize_on)

            # execute post transform processors (sync) passing in deserialized data and the rebuilt meta
            for processor in self.registry['processors']['sync']['post_transform']:
                data = processor(data=data,
                                 meta=dict(pkey=o._pkey, model=o.__qname__, file=o._source, created=o._created))
                # potential future option: pass many is true; but would slow down pipeline

            if data:
                # confirm data is not null to prevent writing blank lines to file
                # this provides the ability to implement post processors which
                # filter the data based on some user-defined criteria

                if isinstance(data, list):
                    # data will be a list if it was denormalized above
                    # the list is split here to maintain json lines (i.e., one object per line)
                    for entry in data:
                        # o is an instance of the specified model (an io.Model)
                        o._data = entry
                        yield o
                else:
                    # o is an instance of the specified model (an io.Model)
                    o._data = data
                    yield o
            else:
                continue

    def _load(self, databundle) -> None:
        for generator, meta in databundle:
            self.db.engine.execute(
                self.tables(meta['model']).__table__.insert(),
                [data for data in generator]
            )

    def _load_processor(self, meta: List[dict]) -> None:
        # execute pre load processors (async)
        for processor in self.registry['processors']['async']['pre_load']:
            self.exec.processor.delay(name=f'{self.__qname__}.pre_load: {processor.__name__}',
                                      func=processor, **dict(meta=meta))

        # execute pre load processors (sync) passing in meta
        for processor in self.registry['processors']['sync']['pre_load']:
            meta = processor(meta=meta)

        data = []
        for entry in meta:
            # build data reader generator list
            data.append((self._reader(entry, stage='load'), entry))

        # execute load method based on params
        if self.registry['params']['load']['many']:
            # no iterator or response expected if many
            self.registry['stages']['load'](data)

        # if not many, check for batch size declaration and execute accordingly
        elif self.registry['params']['load'].get('batches', None):
            while True:
                try:
                    # create an iterator slice from the load generator of the registered size
                    # ensure the generator is not already consumed (errors will be caught by the outer try-except)
                    # execute until the end of the slice, then commit and re-loop
                    loads = islice(self.registry['stages']['load'](data), self.registry['params']['load']['batches'])
                    next(loads)
                    try:
                        while True:
                            next(loads)
                    except StopIteration:
                        self.db.session.commit()
                except StopIteration:
                    break

        # else if batch size not defined, execute completely
        else:
            while True:
                try:
                    next(self.registry['stages']['load'](data))
                except StopIteration:
                    break

        # ensure data is committed (user may have already done this in load or
        # if it was batched above; however, superfluous calls have no effect)
        self.db.session.commit()

        if self.registry['params']['load'].get('record', True):
            for entry in meta:
                # create db record for committed data object
                self.db.upsert(
                    pkey=entry['pkey'], stage='load', file=entry['file'], model=entry['model'], created=entry['created']
                )

        # execute on load commit processor (async)
        for processor in self.registry['processors']['async'][f'on_load_commit']:
            self.exec.processor.delay(name=f'{self.__qname__}.on_load_commit: {processor.__name__}',
                                      func=processor, **dict(meta=meta))

    def _reader(self, meta: dict, stage: str=None, file_kwargs=None, **kwargs) -> Generator:
        """Generator for reading files.

        :type meta: dict(pkey, model, file, created)
        :type file_kwargs: dict, file open kwargs
        :type kwargs: dict, reader kwargs passed to csv.DictReader or json.loads
        """

        # set default file kwargs
        if file_kwargs is None:
            file_kwargs = {}
        file_kwargs.setdefault('mode', 'rt')
        file_kwargs.setdefault('encoding', self.encoding)
        file_kwargs.setdefault('errors', self.encoding_errors)

        # add to session; grab file extensions
        try:
            self.session.read.update({(meta['model'], meta['pkey']): meta['file']})
            ext = meta['file'].rsplit('.', 2)[1:]
        except TypeError:
            # if TypeError raised here then the local meta is a string as the outer for-loop
            # was expecting to iterate over a list of dictionaries but received a single dict
            raise PipelineExecutionError(f"[{self.__qname__}] Invalid meta object provided to the {stage} stage: "
                                         f"ensure meta input is a list of dictionaries.")

        # open file handler using gzip if compressed
        if 'gz' in ext:
            self.session.file[meta['file']] = gzip.open(meta['file'], **file_kwargs)
            self.scopedsession.file[meta['file']] = self.session.file[meta['file']]
        else:
            self.session.file[meta['file']] = open(meta['file'], **file_kwargs)
            self.scopedsession.file[meta['file']] = self.session.file[meta['file']]

        # read data based on type, creates a generator yielding python-type data
        file = csv.DictReader(self.session.file[meta['file']], **kwargs) if 'csv' in ext else self.session.file[meta['file']]
        handler = pass_through if 'csv' in ext else json.loads
        for line in file:
            try:
                yield handler(line)
            except TypeError:
                yield line

        # close file, remove from session (executed when generator terminates)
        self.session.file[meta['file']].close()
        del self.session.read[(meta['model'], meta['pkey'])]
        del self.session.file[meta['file']]
        del self.scopedsession.file[meta['file']]

    def _writer(self, pipeline: Generator, stage: str, autocommit: bool):
        """Consumes a generator, writing data to disk."""
        key = None
        timestamp = None
        meta = []
        commit = set()
        for o in pipeline:
            # o is an instance of the specified model (an io.Model)
            # o._data contains fully-processed data ready for writing
            try:
                # if this does not raise a KeyError then we have already begun
                # writing data to this file, newlines are prepended to incoming
                # data which is written in the output format.
                path = self.session.write[(o.__qname__, o._pkey)]  # todo: joining files, won't work because pkey will be different
                self.session.file[path].write('\n' + self.output(o._data))
            except KeyError:
                # KeyError means the model name and pkey are not currently in the
                # write session: meaning there is not currently a file open for this
                # data object. This is likely the first iteration of the generator.

                # If the key is set, however, this is not the first iteration of the
                # generator meaning a prior data object was yielded and spent; the
                # open file handler can therefore be closed and the object committed
                # (this happens immediately by default if autocommit=True to prevent
                # open files from lingering during a long running task).
                if key:
                    if autocommit:
                        meta.extend(self._commit(key, stage, timestamp))
                    else:
                        commit.add((key, stage, timestamp))

                timestamp = o._created or datetime.utcnow()

                # while writing, files are placed in the temp directory
                # temp files are given a dotted name which represents their final path
                tmp = ".".join(
                    [self.config[f'DATA_{stage.upper()}'].rsplit('/', 1)[1], o.__directory__, timestamp.strftime('%Y'),
                     timestamp.strftime('%m'), timestamp.strftime('%d'), o.__filename__]
                )

                # todo: variable file names
                path = os.path.join(self.config['DATA_TEMP'], tmp)

                # add to session and open file handler based on compression setting
                self.session.write[(o.__qname__, o._pkey)] = path
                if self.compression:
                    self.session.file[path] = gzip.open(path, 'at', encoding=self.encoding, errors=self.encoding_errors)
                else:
                    self.session.file[path] = open(path, 'w', encoding=self.encoding, errors=self.encoding_errors)

                # add reference to scoped session
                self.scopedsession.write[(o.__qname__, o._pkey)] = self.session.write[(o.__qname__, o._pkey)]
                self.scopedsession.file[path] = self.session.file[path]

                # write first line of data in the output format, newline is not needed.
                self.session.file[path].write(self.output(o._data))

                # set key value to current file handler key for check on next KeyError or StopIteration
                key = (o.__qname__, o._pkey)

        if not key:
            # if key is still None then the pipeline generator did not return anything
            # this is likely because of some improperly formatted data passed to a
            # model within the preceding user-implemented stage
            raise PipelineDataError(f"[{self.__qname__}] Invalid data provided to the {stage} stage: "
                                    f"review the files from the prior stage.")

        # commit all or last data object (only one item if autocommit=True)
        commit.add((key, stage, timestamp))
        for entry in commit:
            meta.extend(self._commit(*entry))

        return meta

    def _commit(self, key: tuple, stage: str, timestamp: datetime):
        # grab the path of the completed temp file, unpack key
        tmp = self.session.write[key]
        model, pkey = key

        # close file, remove from sessions
        self.session.file[tmp].close()
        del self.session.write[key]
        del self.session.file[tmp]
        del self.scopedsession.write[key]
        del self.scopedsession.file[tmp]

        # create permanent path, move file (os.renames handles mkdir and rm empty dir)
        path = os.path.join(self.config['DATA'], *os.path.split(tmp)[1].split('.')) + '.jsonl'
        if self.compression:
            path += '.gz'
        os.renames(tmp, path)

        # create db record for committed data object
        meta = self.db.upsert(pkey=pkey, stage=stage, file=path, model=model, created=timestamp)

        # execute on commit processors (async)
        for processor in self.registry['processors']['async'][f'on_{stage}_commit']:
            self.exec.processor.delay(name=f'{self.__qname__}.on_{stage}_commit: {processor.__name__}',
                                      func=processor, **dict(meta=[meta]))

        return [meta]
