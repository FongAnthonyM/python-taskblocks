========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor| |requires|
        | |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/python-processingblocks/badge/?style=flat
    :target: https://python-processingblocks.readthedocs.io/
    :alt: Documentation Status

.. |travis| image:: https://api.travis-ci.com/fonganthonym/python-processingblocks.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.com/github/fonganthonym/python-processingblocks

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/fonganthonym/python-processingblocks?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/fonganthonym/python-processingblocks

.. |requires| image:: https://requires.io/github/fonganthonym/python-processingblocks/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/fonganthonym/python-processingblocks/requirements/?branch=master

.. |codecov| image:: https://codecov.io/gh/fonganthonym/python-processingblocks/branch/master/graphs/badge.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/fonganthonym/python-processingblocks

.. |version| image:: https://img.shields.io/pypi/v/processingblocks.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/processingblocks

.. |wheel| image:: https://img.shields.io/pypi/wheel/processingblocks.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/processingblocks

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/processingblocks.svg
    :alt: Supported versions
    :target: https://pypi.org/project/processingblocks

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/processingblocks.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/processingblocks

.. |commits-since| image:: https://img.shields.io/github/commits-since/fonganthonym/python-processingblocks/v0.1.0.svg
    :alt: Commits since latest release
    :target: https://github.com/fonganthonym/python-processingblocks/compare/v0.1.0...master



.. end-badges

An SDK for creating multiprocess applications that use a block diagram paradigm.

* Free software: BSD 2-Clause License

Installation
============

::

    pip install processingblocks

You can also install the in-development version with::

    pip install https://github.com/fonganthonym/python-processingblocks/archive/master.zip


Documentation
=============


https://python-processingblocks.readthedocs.io/


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
