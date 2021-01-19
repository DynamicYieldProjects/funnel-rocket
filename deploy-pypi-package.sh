#!/usr/bin/env sh

#
# TODO this is just a stub for a real build & deploy thingie :-)
#

pip install twine setuptools wheel

if [ -e build ]; then rm -rf build/ ; fi
if [ -e dist ]; then rm -rf dist/ ; fi
if [ -e funnel_rocket.egg-info ]; then rm -rf funnel_rocket.egg-info/ ; fi

python setup.py sdist bdist_wheel

twine upload --repository testpypi dist/*