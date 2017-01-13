/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef __POOL_HELPER_HPP__
#define __POOL_HELPER_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <AtomicInc.hpp>
#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/FwkStrCvt.hpp"
#include "fwklib/FwkLog.hpp"
#include <gfcpp/PoolFactory.hpp>
#include <stdlib.h>

#include <string>
#include <map>

namespace gemfire {
namespace testframework {

// ----------------------------------------------------------------------------

/** @class PoolHelper
  * @brief Class used to define a valid combination of attributes and
  * specifications for a pool.
  */
class PoolHelper {
  const FwkPool* m_pool;
  std::string m_spec;

 public:
  /** Fill in this instance of PoolHelper based on the spec named by sname.
   */
  PoolHelper(const FrameworkTest* test) : m_pool(NULL) {
    m_spec = test->getStringValue("poolSpec");
    if (m_spec.empty()) {
      FWKEXCEPTION("Failed to find PoolSpec definition.");
    }
    test->bbSet("GFE_BB", "testScheme", " ");
    m_pool = test->getPoolSnippet(m_spec);
    if (m_pool == NULL) {
      FWKEXCEPTION("Failed to find pool definition.");
    }
  }

  const std::string specName() { return m_spec; }

  std::string poolAttributesToString(PoolPtr& pool) {
    std::string sString;
    sString += "\npoolName: ";
    sString += FwkStrCvt(pool->getName()).toString();
    sString += "\nFreeConnectionTimeout: ";
    sString += FwkStrCvt(pool->getFreeConnectionTimeout()).toString();
    sString += "\nLoadConditioningInterval: ";
    sString += FwkStrCvt(pool->getLoadConditioningInterval()).toString();
    sString += "\nSocketBufferSize: ";
    sString += FwkStrCvt(pool->getSocketBufferSize()).toString();
    sString += "\nReadTimeout: ";
    sString += FwkStrCvt(pool->getReadTimeout()).toString();
    sString += "\nMinConnections: ";
    sString += FwkStrCvt(pool->getMinConnections()).toString();
    sString += "\nMaxConnections: ";
    sString += FwkStrCvt(pool->getMaxConnections()).toString();
    sString += "\nStatisticInterval: ";
    sString += FwkStrCvt(pool->getStatisticInterval()).toString();
    sString += "\nRetryAttempts: ";
    sString += FwkStrCvt(pool->getRetryAttempts()).toString();
    sString += "\nSubscriptionEnabled: ";
    sString += pool->getSubscriptionEnabled() ? "true" : "false";
    sString += "\nSubscriptionRedundancy: ";
    sString += FwkStrCvt(pool->getSubscriptionRedundancy()).toString();
    sString += "\nSubscriptionMessageTrackingTimeout: ";
    sString +=
        FwkStrCvt(pool->getSubscriptionMessageTrackingTimeout()).toString();
    sString += "\nSubscriptionAckInterval: ";
    sString += FwkStrCvt(pool->getSubscriptionAckInterval()).toString();
    sString += "\nServerGroup: ";
    sString += pool->getServerGroup();
    sString += "\nIdleTimeout: ";
    sString += FwkStrCvt((int64_t)pool->getIdleTimeout()).toString();
    sString += "\nPingInterval: ";
    sString += FwkStrCvt((int64_t)pool->getPingInterval()).toString();
    sString += "\nThreadLocalConnections: ";
    sString += pool->getThreadLocalConnections() ? "true" : "false";
    sString += "\nMultiuserAuthentication: ";
    sString += pool->getMultiuserAuthentication() ? "true" : "false";
    sString += "\nPRSingleHopEnabled: ";
    sString += pool->getPRSingleHopEnabled() ? "true" : "false";
    sString += "\nLocator: ";
    CacheableStringArrayPtr str =
        dynamic_cast<CacheableStringArray*>(pool->getLocators().ptr());
    if (pool->getLocators() != NULLPTR && pool->getLocators()->length() > 0) {
      for (int32_t stri = 0; stri < str->length(); stri++) {
        sString += str->operator[](stri)->asChar();
        sString += ",";
      }
    }
    sString += "\nServers: ";
    str = dynamic_cast<CacheableStringArray*>(pool->getServers().ptr());
    if (pool->getServers() != NULLPTR && pool->getServers()->length() > 0) {
      for (int32_t stri = 0; stri < str->length(); stri++) {
        sString += str->operator[](stri)->asChar();
        sString += ",";
      }
    }
    sString += "\n";
    return sString;
  }

  PoolPtr createPool() {
    const char* poolName = m_pool->getName().c_str();
    PoolPtr pptr = PoolManager::find(poolName);
    if (pptr == NULLPTR) {
      pptr = m_pool->createPool();
    }
    FWKINFO(" Following are Pool attributes :" << poolAttributesToString(pptr));
    return pptr;
  }
  PoolPtr createPoolForPerf() {
    PoolPtr pptr = m_pool->createPool();
    FWKINFO(" Following are Pool attributes :" << poolAttributesToString(pptr));
    return pptr;
  }
};

}  // namespace testframework
}  // namespace gemfire

// ----------------------------------------------------------------------------

#endif  // __POOL_HELPER_HPP__
