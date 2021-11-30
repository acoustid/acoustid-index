// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "util/options.h"

#include <gtest/gtest.h>

#include "util/test_utils.h"

using namespace Acoustid;

TEST(OptionParserTest, Basic) {
    int argc = 9;
    char *argv[] = {strdup("./test"), strdup("-a"), strdup("--bbb"), strdup("--ccc=x"), strdup("--ddd"),
                    strdup("y"),      strdup("-e"), strdup("z"),     strdup("file")};
    OptionParser parser("%prog [options]");
    parser.addOption("aaa", 'a');
    parser.addOption("bbb", 'b');
    parser.addOption("ccc", 'c').setArgument();
    parser.addOption("ddd", 'd').setArgument();
    parser.addOption("eee", 'e').setArgument();
    Options *options = parser.parse(argc, argv);
    EXPECT_FALSE(options->contains("000"));
    EXPECT_TRUE(options->contains("aaa"));
    EXPECT_TRUE(options->contains("bbb"));
    EXPECT_STREQ("x", qPrintable(options->option("ccc")));
    EXPECT_STREQ("y", qPrintable(options->option("ddd")));
    EXPECT_STREQ("z", qPrintable(options->option("eee")));
    EXPECT_EQ(1, options->argumentCount());
    EXPECT_STREQ("file", qPrintable(options->argument(0)));
    delete options;
    for (int i = 0; i < argc; i++) {
        free(argv[i]);
    }
}
