#!/usr/bin/env bash

# ./rename-go-module.sh example.com/old/module example.com/new-module

go mod edit -module "${2}"
find . -type f -name '*.go' -exec sed -i -e "s,\"${1}/,\"${2}/,g" {} \;
find . -type f -name '*.go' -exec sed -i -e "s,\"${1}\",\"${2}\",g" {} \;
