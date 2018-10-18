from blinker import NamedSignal


class Signal(dict):
    """Accessibility for your signals. Provides a mapping of signal names
    to their corresponding blinker signal and register method for easy creation.
    Returned objects are blinker Signal objects, with access to all documented
    methods.

    Usage Examples:
    .. code-block:: python
        >>>from flask import Flask
        >>>app = Flask(__name__)

        >>>app.signal = Signal()
        >>>app.signal.register('job_complete')

        >>>def run_job(obj):
        >>>    ...
        >>>    app.signal.job_complete.send(obj)

    See blinker documentation for more usage examples.
    """

    def register(self, name, doc=None, subscriber=None):
        self.update({name: NamedSignal(name, doc)})
        if subscriber:
            self[name].connect(subscriber)

    def __getattr__(self, name):
        try:
            return self.__getitem__(name)
        except KeyError as error:
            raise KeyError(f'Unregistered signal requested: {name}') from error

    def __bool__(self):
        return True
