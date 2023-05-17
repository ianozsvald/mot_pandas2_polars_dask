#!/bin/bash

cd data
gunzip -kv *.gz
unzip -aL \*.zip -x '__MACOSX/*' -d unzipped
