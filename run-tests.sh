#!/bin/sh

make && ./tests --gtest_print_time --gtest_filter="*$1*"

