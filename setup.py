"""
Flask-Data-Pipes
----------------

Simple, performant data pipelines
"""
from setuptools import setup, find_packages


setup(
    name='Flask-Data-Pipes',
    version='1.0.3a',
    url='https://github.com/Nasdaq/flask-data-pipes',
    license='APACHE-2.0',
    author='Jonathon Scarbeau',
    author_email='jonathon.scarbeau@nasdaq.com',
    description='Simple, performant data pipelines',
    long_description=__doc__,
    packages=find_packages(exclude=['examples', ]),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    python_requires='>=3.6',
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
