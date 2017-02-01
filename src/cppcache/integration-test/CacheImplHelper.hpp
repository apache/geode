#pragma once

#ifndef GEODE_INTEGRATION_TEST_CACHEIMPLHELPER_H_
#define GEODE_INTEGRATION_TEST_CACHEIMPLHELPER_H_

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

#include <gfcpp/GeodeCppCache.hpp>
#include <stdlib.h>
#include <gfcpp/SystemProperties.hpp>
#include <ace/OS.h>
#include "testUtils.hpp"

#ifndef ROOT_NAME
ROOT_NAME++ +
    DEFINE ROOT_NAME before including CacheHelper.hpp
#endif

#ifndef ROOT_SCOPE
#define ROOT_SCOPE LOCAL
#endif

    using namespace apache::geode::client;
using namespace unitTests;

class CacheImplHelper : public CacheHelper {
 public:
  CacheImplHelper(const char* member_id,
                  const PropertiesPtr& configPtr = NULLPTR)
      : CacheHelper(member_id, configPtr) {}

  CacheImplHelper(const PropertiesPtr& configPtr = NULLPTR)
      : CacheHelper(configPtr) {}

  virtual void createRegion(const char* regionName, RegionPtr& regionPtr,
                            uint32_t size, bool ack = false,
                            bool cacheServerClient = false,
                            bool cacheEnabled = true) {
    RegionAttributesPtr regAttrs;
    AttributesFactory attrFactory;
    // set lru attributes...
    attrFactory.setLruEntriesLimit(0);     // no limit.
    attrFactory.setInitialCapacity(size);  // no limit.
    // then...
    attrFactory.setCachingEnabled(cacheEnabled);
    regAttrs = attrFactory.createRegionAttributes();
    showRegionAttributes(*regAttrs);
    CacheImpl* cimpl = TestUtils::getCacheImpl(cachePtr);
    ASSERT(cimpl != NULL, "failed to get cacheImpl *.");
    cimpl->createRegion(regionName, regAttrs, regionPtr);
    ASSERT(regionPtr != NULLPTR, "failed to create region.");
  }
};

#endif // GEODE_INTEGRATION_TEST_CACHEIMPLHELPER_H_
