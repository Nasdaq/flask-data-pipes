from flask import current_app as app
from ..models.objects import DataObject
from ..data_pipes.exceptions import StopPipeline, PipelineVersionError


@app.celery.task(serializer='pickle')
def pipeline_task(meta, name, func_list):
    app.logger.info(f"Pipeline Task Received: {name}")
    response = meta
    for func in func_list:
        response = func(response)

    return response


@app.celery.task(serializer='pickle')
def processor_task(name, func, **kwargs):
    app.logger.info(f"Pipeline Processor Task Received: {name}")
    return func(**kwargs)


@app.celery.task
def restart_stalled_pipelines():
    data_objects = DataObject.query.filter(DataObject.pipeline_completed is not True).all()
    for obj in data_objects:
        try:
            obj.advance()
        except (StopPipeline, PipelineVersionError):
            continue
