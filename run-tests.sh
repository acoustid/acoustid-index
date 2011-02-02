#!/bin/sh

make
./tests --gtest_filter="*$1*"

