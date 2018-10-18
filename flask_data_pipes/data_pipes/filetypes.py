from flask import current_app as app
from enum import Enum


class FileType(Enum):
    any = app.config['ACCEPT']['ANY']
    data = app.config['ACCEPT']['DATA']
    image = app.config['ACCEPT']['IMAGE']
    document = app.config['ACCEPT']['DOCUMENT']
    archive = app.config['ACCEPT']['ARCHIVE']

    csv = ['csv']
    json = ['json']
    zip = ['zip']
