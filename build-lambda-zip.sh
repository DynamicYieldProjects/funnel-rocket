#!/usr/bin/env sh
#
# TODO this is just a stub for a real build & deploy thingie :-)
#

PACKAGE_FILE=lambda-package.zip
if [ -e $PACKAGE_FILE ]; then rm $PACKAGE_FILE; fi

# TODO find -E only works on Mac...
# shellcheck disable=SC2046
zip $PACKAGE_FILE $(find -E . -regex './frocket/.*\.(py|json)')