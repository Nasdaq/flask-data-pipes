"""
Flask-Data-Pipes
----------------

Simple, performant data pipelines
"""
from setuptools import setup


setup(
    name='Flask-Data-Pipes',
    version='1.0a',
    url='',
    license='',
    author='Jonathon Scarbeau',
    author_email='jonathon.scarbeau@nasdaq.com',
    description='Simple, performant data pipelines',
    long_description=__doc__,
    packages=['flask_data_pipes'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'Flask',
        'Flask-SQLAlchemy',
        'blinker',
        'celery',
        'marshmallow',
        'inflection',
        'requests'
    ],
    classifiers=[
    ]
)
