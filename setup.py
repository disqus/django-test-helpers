#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='django-test-helpers',
    version='0.1.0',
    author='DISQUS',
    author_email='opensource@disqus.com',
    url='http://github.com/disqus/django-test-helpers',
    packages=find_packages(exclude=["tests"]),
    zip_safe=False,
    license='Apache License 2.0',
    include_package_data=True,
)
