#pragma once

#ifndef APACHE_GEODE_GUARD_d8d76480867ce6629add87fe227d225f
#define APACHE_GEODE_GUARD_d8d76480867ce6629add87fe227d225f

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

/**
  * @file    Security.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------


// ----------------------------------------------------------------------------

#include <gfcpp/GeodeCppCache.hpp>

#include <gfcpp/CacheableString.hpp>
#include <gfcpp/Cache.hpp>
#include <gfcpp/Region.hpp>

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TestClient.hpp"

#include <cstdlib>

#ifdef WIN32
#ifdef llabs
#undef llabs
#endif
#define llabs(x) (((x) < 0) ? ((x) * -1LL) : (x))
#endif

#define LAT_MARK 0x55667788

namespace apache {
namespace geode {
namespace client {
namespace testframework {
namespace security {

class Security : public FrameworkTest {
 public:
  Security(const char* initArgs)
      : FrameworkTest(initArgs),
        m_KeysA(NULL),
        m_MaxKeys(0),
        m_KeyIndexBegin(0),
        m_MaxValues(0),
        m_CValue(NULL) {}

  virtual ~Security(void) {
    clearKeys();
    m_CValue = NULL;
  }

  void onRegisterMembers(void);

  int32_t createRegion();
  /*  int32_t puts();
  int32_t gets();
  int32_t destroys();
  int32_t netsearch();
 */ int32_t
  checkValues();
  int32_t populateRegion();
  int32_t registerInterestList();
  int32_t registerRegexList();
  int32_t unregisterRegexList();
  int32_t registerAllKeys();
  int32_t verifyInterestList();
  int32_t doEntryOperations();
  //  int32_t doServerKeys();

  void checkTest(const char* taskId);
  void getClientSecurityParams(PropertiesPtr prop, std::string credentials);
  CacheablePtr getUserObject(const std::string& objType);
  CacheableStringPtr getKey(int32_t max);
  void runQuery(int32_t& queryCnt);

 private:
  int32_t initKeys(bool useDefault = true);
  void clearKeys();
  void initStrKeys(int32_t low, int32_t high, const std::string& keyBase);
  int32_t initValues(int32_t num, int32_t siz = 0, bool useDefault = true);
  RegionPtr getRegionPtr(const char* reg = NULL);
  bool checkReady(int32_t numClients);

  CacheableStringPtr* m_KeysA;
  int32_t m_MaxKeys;
  int32_t m_KeyIndexBegin;
  int32_t m_MaxValues;

  CacheableBytesPtr* m_CValue;
};

}  //   namespace security
}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache
// ----------------------------------------------------------------------------


#endif // APACHE_GEODE_GUARD_d8d76480867ce6629add87fe227d225f
