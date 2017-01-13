#ifndef _GEMFIRE_STATISTICS_STATSDEF_HPP_
#define _GEMFIRE_STATISTICS_STATSDEF_HPP_

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/** @file
*/

namespace gemfire_statistics {

#define MAX_1BYTE_COMPACT_VALUE 127
#define MIN_1BYTE_COMPACT_VALUE -128 + 7
#define MAX_2BYTE_COMPACT_VALUE 32767
#define MIN_2BYTE_COMPACT_VALUE -32768
#define COMPACT_VALUE_2_TOKEN -128
#define COMPACT_VALUE_3_TOKEN -128 + 1
#define COMPACT_VALUE_4_TOKEN -128 + 2
#define COMPACT_VALUE_5_TOKEN -128 + 3
#define COMPACT_VALUE_6_TOKEN -128 + 4
#define COMPACT_VALUE_7_TOKEN -128 + 5
#define COMPACT_VALUE_8_TOKEN -128 + 6
#define ILLEGAL_STAT_OFFSET 255

#define MAX_DESCRIPTORS_PER_TYPE 254

typedef enum {

  GFS_OSTYPE_LINUX = 0,
  GFS_OSTYPE_SOLARIS = 1,
  GFS_OSTYPE_WINDOWS = 2,
  GFS_OSTYPE_MACOSX = 3

} GFS_OSTYPES;
};
#endif  // _GEMFIRE_STATISTICS_STATSDEF_HPP_
