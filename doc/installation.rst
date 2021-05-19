Installation
============

Diepvries is a Python library with very few dependencies, packaged
with `pip`. The recommended way to install it is through a Python
wheel, by pointing your dependency manager to diepvries.

Diepvries releases are versioned according to `Semantic versioning
<https://semver.org/>`_. This means you can use the compatibility
operator when specifying the version you want to use.

Installation with pipenv
------------------------

.. code-block:: shell

    pipenv install diepvries

Installation with poetry
------------------------

.. code-block:: shell

    poetry add diepvries

Installation with bare pip
--------------------------

.. code-block:: shell

    pip install diepvries

Installing a development version
--------------------------------

This installation method should be used when developing on Diepvries,
not for regular use.

.. code-block:: shell

    git clone git@github.com:PicnicSuperMarket/diepvries.git
    cd diepvries
    python3 -m venv venv
    source ./venv/bin/activate
    python3 setup.py develop