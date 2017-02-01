#pragma once

#ifndef GEODE_STATISTICS_STATSDEF_H_
#define GEODE_STATISTICS_STATSDEF_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** @file
*/

namespace apache {
namespace geode {
namespace statistics {

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
}  // namespace statistics
}  // namespace geode
}  // namespace apache

#endif // GEODE_STATISTICS_STATSDEF_H_
