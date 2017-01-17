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
