/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "Version.hpp"
#include "CacheImpl.hpp"

#define VERSION_ORDINAL_56 0
#define VERSION_ORDINAL_57 1
#define VERSION_ORDINAL_TEST 2   // for GFE dunit testing
#define VERSION_ORDINAL_58 3     // since NC 3000
#define VERSION_ORDINAL_603 4    // since NC 3003
#define VERSION_ORDINAL_61 5     // since NC ThinClientDelta branch
#define VERSION_ORDINAL_65 6     // since NC 3500
#define VERSION_ORDINAL_651 7    // since NC 3510
#define VERSION_ORDINAL_66 16    // since NC 3600
#define VERSION_ORDINAL_662 17   // since NC 3620
#define VERSION_ORDINAL_6622 18  // since NC 3622
#define VERSION_ORDINAL_7000 19  // since NC 7000
#define VERSION_ORDINAL_7001 20  // since NC 7001
#define VERSION_ORDINAL_80 30    // since NC 8000
#define VERSION_ORDINAL_81 35    // since NC 8100

#define VERSION_ORDINAL_NOT_SUPPORTED 59  // for GFE dunit testing

using namespace gemfire;

int8_t Version::m_ordinal = VERSION_ORDINAL_81;
