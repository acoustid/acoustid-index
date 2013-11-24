// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "acoustid_document_handler.h"

using namespace Acoustid;

TEST(AcoustIdDocumentHandlerTest, ExtractQuery)
{
	AcoustIdDocumentHandler handler(2, 5, 28);

	Document doc = Document()
		<< 2775975844
		<< 2783315860
		<< 2783387525
		<< 2766613893
		<< 2766498263
		<< 2766465334
		<< 2783242518
		<< 2783423750
		<< 2816976134
		<< 2816783622
		<< 2792657158
		<< 2788462855;

	Document expectedQuery = Document()
		<< 2783387520
		<< 2766613888
		<< 2766498256
		<< 2766465328
		<< 2783242512;
	Document query = handler.extractQuery(doc);
	ASSERT_EQ(query.size(), expectedQuery.size());
	ASSERT_INTARRAY_EQ(query, expectedQuery, expectedQuery.size());
}

TEST(AcoustIdDocumentHandlerTest, ExtractQueryWithSilence)
{
	AcoustIdDocumentHandler handler(2, 5, 28);

	Document doc = Document()
		<< 2775975844
		<< 627964279
		<< 2783315860
		<< 2783387525
		<< 627964279
		<< 2766613893
		<< 2766498263
		<< 627964279
		<< 2766465334
		<< 2783242518
		<< 627964279
		<< 2783423750
		<< 2816976134
		<< 627964279
		<< 2816783622
		<< 2792657158
		<< 2788462855;

	Document expectedQuery = Document()
		<< 2783315856
		<< 2783387520
		<< 2766613888
		<< 2766498256
		<< 2766465328;

	Document query = handler.extractQuery(doc);
	ASSERT_EQ(query.size(), expectedQuery.size());
	ASSERT_INTARRAY_EQ(query, expectedQuery, expectedQuery.size());
}

TEST(AcoustIdDocumentHandlerTest, ExtractQueryWithDuplicates)
{
	AcoustIdDocumentHandler handler(2, 5, 28);

	Document doc = Document()
		<< 2775975844
		<< 2783315860
		<< 2783387525
		<< 2783387526
		<< 2783387527
		<< 2766613893
		<< 2766498263
		<< 2766465334
		<< 2783242518
		<< 2783423750
		<< 2816976134
		<< 2816783622
		<< 2792657158
		<< 2788462855;

	Document expectedQuery = Document()
		<< 2783387520
		<< 2766613888
		<< 2766498256
		<< 2766465328
		<< 2783242512;

	Document query = handler.extractQuery(doc);
	ASSERT_EQ(query.size(), expectedQuery.size());
}
