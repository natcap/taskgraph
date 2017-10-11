"""taskgraph setup.py."""
from setuptools import setup

README = open('README.rst').read()

setup(
    name='taskgraph',
    natcap_version='taskgraph/version.py',
    description='Parallel task graph framework.',
    long_description=README,
    maintainer='Rich Sharp',
    maintainer_email='richpsharp@gmail.com',
    url='https://bitbucket.org/richsharp/taskgraph',
    packages=['taskgraph'],
    license='BSD',
    keywords='parallel multiprocessing distributed computing',
    extras_require={
        'niced_processes': ['psutil'],
        },
    classifiers=[
        'Intended Audience :: Developers',
        'Development Status :: 5 - Production/Stable',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2 :: Only',
        'License :: OSI Approved :: BSD License'
    ])
