from setuptools import setup

cmdclass = {}

try:
    from sphinx.setup_command import BuildDoc
    cmdclass['build_sphinx'] = BuildDoc
except ImportError:
    pass

version = "0.1"

setup(
    name="phoenixdb",
    version=version,
    cmdclass=cmdclass,
    description="Phoenix database interface library",
    author="Lukas Lalinsky",
    author_email="lukas@oxygene.sk",
    license="Apache-2",
    packages=["phoenixdb"],
    command_options={
        'build_sphinx': {
            'version': ('setup.py', version),
            'release': ('setup.py', version),
        },
    },
)
