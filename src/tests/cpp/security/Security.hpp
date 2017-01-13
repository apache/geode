/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    Security.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __SECURITY_HPP__
#define __SECURITY_HPP__

// ----------------------------------------------------------------------------

#include <gfcpp/GemfireCppCache.hpp>

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

namespace gemfire {
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
}  // namespace gemfire
// ----------------------------------------------------------------------------

#endif  // __SECURITY_HPP__
