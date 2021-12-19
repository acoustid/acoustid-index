#!/usr/bin/env bash

set -euo pipefail

cd $(dirname $0)

rm -rf sqlite
rm -rf sqlite-amalgamation-*
rm -f sqlite.zip

# https://www.sqlite.org/download.html
curl -L -o sqlite.zip https://www.sqlite.org/2021/sqlite-amalgamation-3370000.zip
unzip sqlite.zip

mv sqlite-amalgamation-* sqlite
git add -N sqlite
