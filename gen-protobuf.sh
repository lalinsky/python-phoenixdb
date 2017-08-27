#!/usr/bin/env bash

AVATICA_VER=rel/avatica-1.10.0

set -e

rm -rf avatica-tmp

mkdir avatica-tmp
cd avatica-tmp
wget -O avatica.tar.gz https://github.com/apache/calcite-avatica/archive/$AVATICA_VER.tar.gz
tar -x --strip-components=1 -f avatica.tar.gz

cd ..
rm -f phoenixdb/avatica/proto/*_pb2.py
protoc --proto_path=avatica-tmp/core/src/main/protobuf/ --python_out=phoenixdb/avatica/proto avatica-tmp/core/src/main/protobuf/*.proto
sed -i 's/import common_pb2/from . import common_pb2/' phoenixdb/avatica/proto/*_pb2.py

rm -rf avatica-tmp
