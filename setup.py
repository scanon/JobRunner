#!/usr/bin/env python
from __future__ import print_function

import os
import sys

from distutils.core import setup

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))

# Get the current package version.
version_ns = {}
with open(pjoin(here, 'version.py')) as f:
    exec(f.read(), {}, version_ns)

setup_args = dict(
    name='JobRunner',
    packages=['JobRunner', 'clients'],
    scripts=['scripts/jobrunner.py',
             'scripts/slurm_submit',
             'scripts/wdl_run',
             'scripts/slurm_checkjob'],
    version=version_ns['__version__'],
    description="""KBase Job Runner used to run KBase
                apps
                """,
    long_description="""KBase Job Runner""",
    author="KBase Team",
    author_email="scanon@lbl.gov",
    url="https://kbase.us",
    license="BSD",
    platforms="Linux, Mac OS X",
    keywords=['Utilities', 'Containers', 'Condor'],
    classifiers=[
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
)

# setuptools requirements
if 'setuptools' in sys.modules:
    setup_args['install_requires'] = install_requires = []
    with open('requirements.txt') as f:
        for line in f.readlines():
            req = line.strip()
            if not req or req.startswith(('-e', '#')):
                continue
            install_requires.append(req)


def main():
    setup(**setup_args)

if __name__ == '__main__':
    main()
