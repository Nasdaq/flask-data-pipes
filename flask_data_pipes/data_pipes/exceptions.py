class PipelineError(NameError):
    """Core pipeline error."""
    pass


class PipelineModelError(PipelineError):
    """Raised when an invalid operation is performed on the pipeline
    model registry.
    """
    pass


class PipelineTaskSchemaError(PipelineError):
    """Raised when an invalid task schema is declared on a pipeline:
    tasks are enabled without their preceding dependant tasks (e.g.,
    load is set to true but transform is false).
    """
    pass


class PipelineExecutionError(PipelineError):
    """Raised when an invalid pipeline execution is attempted."""
    pass


class PipelineDataError(PipelineError):
    """Raised when an invalid pipeline execution is attempted."""
    pass


class StopPipeline(PipelineError):
    """Raised when execution is attempted on a completed pipeline."""
    pass


class PipelineVersionError(PipelineError):
    """Raised when execution is attempted on an existing data object
    registered to an outdated pipeline or no longer existing model."""
    pass


class ModelError(NameError):
    """Core model error."""
    pass


class ModelFieldDeclarationError(ModelError):
    """Raised due to an attempted invalid field declaration."""
    pass
