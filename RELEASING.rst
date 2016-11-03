Releasing a new version
=======================

Change the version number ``setup.py`` and ``NEWS.rst``.

Commit the changes and tag the repository::

    git tag -s vX.Y

Upload the package to PyPI::

    python setup.py clean sdist upload

Build and upload the documentation::

    python setup.py build_sphinx upload_sphinx
