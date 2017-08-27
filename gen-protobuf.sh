#!/usr/bin/env bash

AVATICA_VER=rel/avatica-1.10.0

set -e

rm -rf phoenixdb/calcite
rm -rf calcite-tmp

mkdir calcite-tmp
cd calcite-tmp
wget -O avatica.tar.gz https://github.com/apache/calcite-avatica/archive/$AVATICA_VER.tar.gz
tar -x --strip-components=1 -f avatica.tar.gz

cd ..
mkdir -p phoenixdb/calcite
protoc --proto_path=calcite-tmp/core/src/main/protobuf/ --python_out=phoenixdb/calcite calcite-tmp/core/src/main/protobuf/*.proto
sed -i 's/import common_pb2/from . import common_pb2/' phoenixdb/calcite/*_pb2.py

rm -rf calcite-tmp

echo '' >> phoenixdb/calcite/__init__.py
