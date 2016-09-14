#!/usr/bin/env bash

export CALCITE_VER=calcite-1.8.0
export CALCITE_DIR=calcite1_8

rm -rf phoenixdb/schema/$CALCITE_DIR
rm -rf calcite-tmp

git init calcite-tmp
cd calcite-tmp
git remote add origin https://github.com/apache/calcite/
git config core.sparsecheckout true
echo "avatica/core/src/main/protobuf/*" >> .git/info/sparse-checkout
git pull --depth=1 origin $CALCITE_VER

cd ..
mkdir -p phoenixdb/schema/$CALCITE_DIR
protoc --proto_path=calcite-tmp/avatica/core/src/main/protobuf/ --python_out=phoenixdb/schema/$CALCITE_DIR calcite-tmp/avatica/core/src/main/protobuf/*.proto

rm -rf calcite-tmp

echo '' >> phoenixdb/schema/__init__.py
echo '' >> phoenixdb/schema/$CALCITE_DIR/__init__.py
