from flask import current_app as app
from .exceptions import StopPipeline, PipelineVersionError
from ..ext.services import get_service


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
    etl = get_service('etl')
    data_objects = etl._DataObject.query.filter(etl._DataObject.pipeline_completed is not True).all()
    for obj in data_objects:
        try:
            obj.advance()
        except (StopPipeline, PipelineVersionError):
            continue
