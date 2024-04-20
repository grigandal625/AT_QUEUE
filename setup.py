from setuptools import setup, find_packages
import json
import os

import pathlib

import pkg_resources

root = os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir))
os.chdir(root)


# old
def read_pipenv_dependencies(fname):
    filepath = os.path.join(os.path.dirname(__file__), fname)
    with open(filepath) as lockfile:
        lockjson = json.load(lockfile)
        return [dependency for dependency in lockjson.get('default')]

# new

with pathlib.Path('requirements.txt').open() as requirements_txt:
    install_requires = [
        str(requirement)
        for requirement
        in pkg_resources.parse_requirements(requirements_txt)
    ]  

if __name__ == '__main__':
    setup(
        name='at-queue',
        version=os.getenv('PACKAGE_VERSION', '0.0.dev1'),
        packages=find_packages(where='src'),
        package_dir={'': 'src'},
        description='AT-TECHNOLOGY message queue component.',
        install_requires=install_requires
    )