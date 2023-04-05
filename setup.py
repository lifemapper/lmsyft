# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='t-rex',
    version='0.1.0',
    description='Package of Lifemapper Broker and Resolver services',
    long_description=readme,
    author='Lifemapper Team',
    author_email='aimeestewart@ku.edu',
    url='https://github.com/lifemapper/t-rex',
    license=license,
    packages=find_packages(exclude=('data', 'docs', 'solrcores', 'tests')),
    install_requires=[
        'cherrypy>=18.6.0',
        'requests>=2.25.1',
        'gdal>=1.11.4']
)
