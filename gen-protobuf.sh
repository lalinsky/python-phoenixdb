#!/usr/bin/env bash

export CALCITE_VER=calcite-1.8.0

rm -rf phoenixdb/calcite
rm -rf calcite-tmp

git init calcite-tmp
cd calcite-tmp
git remote add origin https://github.com/apache/calcite/
git config core.sparsecheckout true
echo "avatica/core/src/main/protobuf/*" >> .git/info/sparse-checkout
git pull --depth=1 origin $CALCITE_VER

cd ..
mkdir -p phoenixdb/calcite
protoc --proto_path=calcite-tmp/avatica/core/src/main/protobuf/ --python_out=phoenixdb/calcite calcite-tmp/avatica/core/src/main/protobuf/*.proto
sed -i 's/import common_pb2/from . import common_pb2/' phoenixdb/calcite/*_pb2.py

rm -rf calcite-tmp

echo '' >> phoenixdb/calcite/__init__.py
