import functools

__all__ = [
    'pre_upload',
    'upload',
    'pre_extract',
    'extract',
    'post_extract',
    'pre_transform',
    'transform',
    'post_transform',
    'pre_load',
    'load',
    'on_upload_commit',
    'on_extract_commit',
    'on_transform_commit',
    'on_load_commit'
]

UPLOAD = 'upload'
EXTRACT = 'extract'
TRANSFORM = 'transform'
LOAD = 'load'

PRE_UPLOAD = 'pre_upload'
PRE_EXTRACT = 'pre_extract'
POST_EXTRACT = 'post_extract'
PRE_TRANSFORM = 'pre_transform'
POST_TRANSFORM = 'post_transform'
PRE_LOAD = 'pre_load'
ON_UPLOAD_COMMIT = 'on_upload_commit'
ON_EXTRACT_COMMIT = 'on_extract_commit'
ON_TRANSFORM_COMMIT = 'on_transform_commit'
ON_LOAD_COMMIT = 'on_load_commit'

__SYNC_TAGS__ = \
    [
        PRE_UPLOAD,
        PRE_EXTRACT,
        POST_EXTRACT,
        PRE_TRANSFORM,
        POST_TRANSFORM,
        PRE_LOAD
    ]

__ASYNC_TAGS__ = \
    [
        PRE_UPLOAD,
        PRE_EXTRACT,
        PRE_TRANSFORM,
        PRE_LOAD,
        ON_UPLOAD_COMMIT,
        ON_EXTRACT_COMMIT,
        ON_TRANSFORM_COMMIT,
        ON_LOAD_COMMIT
    ]


def pre_upload(func=None, async=False):
    """
    Register a method to invoke *before* upload. The method
    receives a dictionary of the file attrs. By default,
    methods are executed synchronously (unless async=True) and
    responsible for returning the file attr dictionary for use
    by the upload method. Data returned by async methods is not
    returned to the pipeline.

    Note:
        Async preprocessors are executed prior to synchronous preprocessors

    Usage:
        ```
        @pre_upload
        def check_inbound_files(self, filename: str, model: str, created, **kwargs) -> dict:
            # kwargs is the form data from the upload
            filename += kwargs['meta']['descriptor']

            return dict(filename=filename, model=model, created=created, **kwargs)
        ```
    """

    return tag_processor(PRE_UPLOAD, func, async)


def upload(func=None, **kwargs):
    """
    Register a method to execute file uploads. The registered
    method will take the place of `_upload` on the core Pipeline
    object.
    """
    return tag_processor(UPLOAD, func, async=False, many=False, **kwargs)


def pre_extract(func=None, async=False):
    """
    Register a method to invoke *before* extraction. The method
    receives the list of meta dictionaries via kwargs. By default,
    methods are executed synchronously (unless async=True) and
    responsible for returning a list of meta dictionaries for use
    by the extract method. Data returned by async methods is not
    returned to the pipeline.

    Note:
        Async preprocessors are executed prior to synchronous preprocessors

    Usage:
        ```
        @pre_extract
        def check_inbound_files(self, meta: list) -> list:
            if len(meta) == 1:
                other = DataObject.query.get('other_needed_file')
                if other:
                    meta.append(dict(
                        pkey=other.pkey, model=other.model,
                        file=other.upload_file, created=other.created
                    ))
                else:
                    break

            return meta
        ```
    """

    return tag_processor(PRE_EXTRACT, func, async)


def extract(func=None, many=False, **kwargs):
    """
    Register a method to execute extraction. The registered
    method will take the place of `_extract` on the core Pipeline
    object.

    :param many: bool, designates whether a single data dict with
    its meta is pass to the decorated function or if the whole
    List[Tuple[Generator: from the file reader, dict: the meta dict]
    object is pass for user manipulation.
    """
    return tag_processor(EXTRACT, func, async=False, many=many, **kwargs)


def post_extract(func=None):
    """
    Register a method to invoke *after* extraction. The method
    receives the data dictionary and meta dictionary via kwargs.
    Methods are executed synchronously and responsible for returning
    the data dictionary for use by the extract method.

    Use ``on_extract_commit`` to invoke post extract functions asynchronously.

    Note:
        Method receives data and meta, only data should be returned.

    Usage:
        ```
        @post_extract
        def append_timestamp(self, data: dict, meta: dict) -> dict:
            data.update({'observed': meta['created']})

            return data
        ```
    """

    return tag_processor(POST_EXTRACT, func, async=False)


def pre_transform(func=None, async=False):
    """
    Register a method to invoke *before* transformation. The method
    receives the list of meta dictionaries via kwargs. By default,
    methods are executed synchronously (unless async=True) and
    responsible for returning a list of meta dictionaries for use
    by the transform method. Data returned by async methods is not
    returned to the pipeline.

    Note:
        Async preprocessors are executed prior to synchronous preprocessors

    Usage:
        ```
        @pre_transform
        def check_inbound_files(self, meta: list) -> list:
            if len(meta) == 1:
                other = DataObject.query.get('other_needed_file')
                if other:
                    meta.append(dict(
                        pkey=other.pkey, model=other.model,
                        file=other.extract_file, created=other.created
                    ))
                else:
                    break

            return meta
        ```
    """

    return tag_processor(PRE_TRANSFORM, func, async)


def transform(func=None, many=False, **kwargs):
    """
    Register a method to execute transformation. The registered
    method will take the place of `_transform` on the core Pipeline
    object.

    :param many: bool, designates whether a single data dict with
    its meta is pass to the decorated function or if the whole
    List[Tuple[Generator: from the file reader, dict: the meta dict]
    object is pass for user manipulation.
    """
    return tag_processor(TRANSFORM, func, async=False, many=many, **kwargs)


def post_transform(func=None):
    """
    Register a method to invoke *after* transformation. The method
    receives the data dictionary and meta dictionary via kwargs.
    Methods are executed synchronously and responsible for returning
    the data dictionary for use by the transform method.

    Use ``on_transform_commit`` to invoke post transform functions asynchronously.

    Note:
        Method receives data and meta, only data should be returned.

    Usage:
        ```
        @post_transform
        def filter_noise(self, data: dict, meta: dict) -> dict:
            if data.get('dB', 0) > 20:
                return None

            else:
                return data
        ```
    """

    return tag_processor(POST_TRANSFORM, func, async=False)


def pre_load(func=None, async=False):
    """
    Register a method to invoke *before* load. The method receives
    the list of meta dictionaries via kwargs. By default,
    methods are executed synchronously (unless async=True) and
    responsible for returning a list of meta dictionaries for use
    by the load method. Data returned by async methods is not
    returned to the pipeline.

    Note:
        Async preprocessors are executed prior to synchronous preprocessors

    Usage:
        ```
        @pre_load
        def check_inbound_files(self, meta: list) -> list:
            if len(meta) == 1:
                other = DataObject.query.get('other_needed_file')
                if other:
                    meta.append(dict(
                        pkey=other.pkey, model=other.model,
                        file=other.extract_file, created=other.created
                    ))
                else:
                    break

            return meta
        ```
    """

    return tag_processor(PRE_LOAD, func, async)


def load(func=None, many=False, record: bool=True, batches: int=None, **kwargs):
    """
    Register a method to execute load. The registered
    method will take the place of `_load` on the core Pipeline
    object and must be implemented if the load stage is True on
    the pipeline declaration.

    :param many: bool, designates whether a single data dict with
    its meta is pass to the decorated function or if the whole
    List[Tuple[Generator: from the file reader, dict: the meta dict]
    object is pass for user manipulation.

    :param batches: int, designate load batch size (many=False only)

    Usage:
        ```
        @load(many=False, batches=10000)
        def load(self, data: dict, meta: dict):
            return db.session.merge(self.tables(meta['model'], data))
        ```
    """
    return tag_processor(LOAD, func, async=False, many=many, record=record, batches=batches, **kwargs)


def on_upload_commit(func=None):
    """
    Register a method to invoke asynchronously after upload.
    The method receives the list of meta dictionaries via kwargs.

    Usage:
        ```
        @on_upload_commit
        def send_signal(self, meta: list) -> None:
            app.signal.upload_complete.send(meta['pkey'])
        ```
    """

    return tag_processor(ON_UPLOAD_COMMIT, func, async=True)


def on_extract_commit(func=None):
    """
    Register a method to invoke asynchronously after extract.
    The method receives the list of meta dictionaries via kwargs.

    Usage:
        ```
        @on_upload_commit
        def send_signal(self, meta: list) -> None:
            app.signal.upload_complete.send(meta['pkey'])
        ```
    """
    return tag_processor(ON_EXTRACT_COMMIT, func, async=True)


def on_transform_commit(func=None):
    """
    Register a method to invoke asynchronously after transform.
    The method receives the list of meta dictionaries via kwargs.

    Usage:
        ```
        @on_transform_commit
        def send_signal(self, meta: list) -> None:
            app.signal.transform_complete.send(meta['pkey'])
        ```
    """
    return tag_processor(ON_TRANSFORM_COMMIT, func, async=True)


def on_load_commit(func=None):
    """
    Register a method to invoke asynchronously after load.
    The method receives the list of meta dictionaries via kwargs.

    Usage:
        ```
        @on_load_commit
        def send_signal(self, meta: list) -> None:
            [app.signal.load_complete.send(entry['pkey']) for entry in meta]
        ```
    """
    return tag_processor(ON_LOAD_COMMIT, func, async=True)


def tag_processor(tag_name, func, async, **kwargs):
    """Tags decorated processor function to be picked up later.
    Only works with functions and instance methods. Class and
    static methods are not supported.

    :return: Decorated function if supplied, else this decorator
    with its args bound.
    """
    # Allow using this as either a decorator or a decorator factory.
    if func is None:
        return functools.partial(
            tag_processor, tag_name, async=async, **kwargs
        )

    # Set a pipeline_tags attribute instead of wrapping in some class,
    # because we still want this to end up as a normal (unbound) method.
    try:
        pipeline_tags = func.__pipeline_tags__
    except AttributeError:
        func.__pipeline_tags__ = pipeline_tags = set()
    # Also save the kwargs for the tagged function on
    # __pipeline_kwargs__, keyed by (<tag_name>, <async>)
    try:
        pipeline_kwargs = func.__pipeline_kwargs__
    except AttributeError:
        func.__pipeline_kwargs__ = pipeline_kwargs = {}

    pipeline_tags.add((tag_name, async))
    pipeline_kwargs[(tag_name, async)] = kwargs  # todo: process kwargs on func

    return func
