/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/TestClient.hpp"
#include "fwklib/FwkLog.hpp"
#include <gfcpp/AttributesFactory.hpp>
//#include <gfcpp/RegionAttributes.hpp>
#include <gfcpp/PoolFactory.hpp>
#include "PoolAttributes.hpp"

using namespace gemfire;
using namespace gemfire::testframework;

// ========================================================================

SpinLock FrameworkTest::m_lck;

// ----------------------------------------------------------------------------

FrameworkTest::FrameworkTest(const char* initArgs) {
#ifdef WIN32
  setNewAndDelete();
#endif
  txManager = NULLPTR;
  // parse args into variables,
  char xml[4096];   // xml file name
  char addr[1024];  // ip address, host:port
  int32_t port;     // int TS port number
  int32_t cnt = sscanf(initArgs, "%s %d %s %d", xml, &m_id, addr, &port);
  if (cnt != 4) {
    FWKEXCEPTION("Failed to parse init args: " << initArgs);
  }
  m_bbc = new FwkBBClient(addr);
  m_deltaMicros = 0;
  m_timeSync = new TimeSync(port, (int32_t*)&m_deltaMicros);
  m_coll = new TestDriver(xml);
  TestClient::createTestClient(50, m_id);
  incClientCount();
}

FrameworkTest::~FrameworkTest() {
  if (m_coll != NULL) {
    delete m_coll;
    m_coll = NULL;
  }

  if (m_bbc != NULL) {
    delete m_bbc;
    m_bbc = NULL;
  }

  if (m_timeSync != NULL) {
    m_timeSync->stop();
    delete m_timeSync;
    m_timeSync = NULL;
  }

  if (m_cache != NULLPTR) {
    cacheFinalize();
  }
}

// ----------------------------------------------------------------------------

const FwkRegion* FrameworkTest::getSnippet(const std::string& name) const {
  FwkRegion* value = NULL;
  const FwkData* data = getData(name.c_str());
  if (data) {
    value = const_cast<FwkRegion*>(data->getSnippet());
  }
  std::string bbs("GFE_BB");
  std::string key("testScheme");
  std::string mode = bbGetString(bbs, key);
  std::string tag = getStringValue("TAG");
  std::string poolName = "_Test_Pool";
  bool isDC = getBoolValue("isDurable");
  int32_t redundancyLevel = getIntValue("redundancyLevel");
  std::string TAG("tags");
  std::string Count("count");

  if (mode == "poolwithendpoints" || mode == "poolwithlocator") {
    FWKINFO("Current Scheme::" << mode);
    Attributes* atts = value->getAttributes();
    if (!tag.empty()) {
      poolName.append(tag);
      atts->setPoolName(poolName);
    } else {
      atts->setPoolName(poolName);
    }
  }
  std::string label = "EndPoints";

  if (!tag.empty()) {
    label += "_";
    label += tag;
  }

  std::string endPts;
  std::string bb("GFE_BB");
  std::string cnt("EP_COUNT");
  int32_t epCount = static_cast<int32_t>(bbGet(bb, cnt));
  for (int32_t i = 1; i <= epCount; i++) {
    std::string key = label + "_";
    key.append(FwkStrCvt(i).toString());
    std::string ep = bbGetString(bb, key);
    if (!endPts.empty()) endPts.append(",");
    endPts.append(ep);
  }
  if (!endPts.empty()) {
    Attributes* atts = value->getAttributes();
    if ((atts != NULL) && (!atts->isLocal()) && (!atts->isWithPool()) &&
        (!isDC) && (redundancyLevel <= 0)) {
      // atts->setEndpoints( endPts );
      FWKINFO("Setting EndPoints to: " << endPts);
    }
  }
  return value;
}

std::vector<std::string> FrameworkTest::getRoundRobinEP() const {
  int32_t contactnum = getIntValue("contactNum");
  std::string label = "EndPoints";
  std::vector<std::string> rbEP;
  int32_t epCount = static_cast<int32_t>(bbGet("GFE_BB", "EP_COUNT"));
  if (contactnum < 0) contactnum = epCount;
  ACE_OS::sleep(1);
  std::string currEPKey("CURRENTEP_COUNT");
  int32_t currentEP =
      static_cast<int32_t>(m_bbc->increment("GFERR_BB", currEPKey));
  for (int i = 0; i < contactnum; i++) {
    if (currentEP > epCount) {
      m_bbc->clear("GFERR_BB");
      currentEP = static_cast<int32_t>(m_bbc->increment("GFERR_BB", currEPKey));
    }
    std::string key = label + "_";
    key.append(FwkStrCvt(currentEP).toString());
    std::string ep = bbGetString("GFE_BB", key);
    rbEP.push_back(ep);
    FWKINFO("getRoundRobinEP = " << ep << " key = " << key
                                 << " currentEP = " << currentEP);
  }

  return rbEP;
}
// ----------------------------------------------------------------------------

const FwkPool* FrameworkTest::getPoolSnippet(const std::string& name) const {
  FwkPool* value = NULL;
  const FwkData* data = getData(name.c_str());
  if (data) {
    value = const_cast<FwkPool*>(data->getPoolSnippet());
  }
  // Make sure , locator + servers both are not set
  if (value->isPoolWithServers() && value->isPoolWithLocators()) {
    FWKEXCEPTION("Both Servers and Locators Can't be specified for Pool.");
  }
  if (value->isPoolWithServers()) {
    std::string tag = getStringValue("TAG");
    std::string label = "EndPoints";

    if (!tag.empty()) {
      label += "_";
      label += tag;
    }
    std::string bb("GFE_BB");
    std::string cnt("EP_COUNT");

    int32_t epCount = static_cast<int32_t>(bbGet(bb, cnt));
    bool disableShufflingEP = getBoolValue("disableShufflingEP");
    if (disableShufflingEP) {
      FWKINFO("disable Shuffling EndPoint is true");
      std::vector<std::string> rbep = getRoundRobinEP();
      for (uint32_t cnt = 0; cnt < rbep.size(); cnt++) {
        value->addServer(rbep[cnt]);
      }
    } else {
      for (int32_t i = 1; i <= epCount; i++) {
        std::string key = label + "_";
        key.append(FwkStrCvt(i).toString());
        std::string ep = bbGetString(bb, key);
        value->addServer(ep);
      }
    }
  } else {
    // add locators
    std::string tag = getStringValue("TAG");
    std::string label = "LOCPORTS";

    if (!tag.empty()) {
      label += "_";
      label += tag;
    }
    std::string bb("GFE_BB");
    std::string cnt("LOC_CNT");

    int32_t locCount = static_cast<int32_t>(bbGet(bb, cnt));
    FWKINFO("LOC_CNT: " << locCount);
    for (int32_t i = 1; i <= locCount; i++) {
      std::string key = label + "_";
      key.append(FwkStrCvt(i).toString());
      std::string loc = bbGetString(bb, key);
      FWKINFO("Locator Key: " << key << "Value:" << loc);
      value->addLocator(loc);
    }
  }

  return value;
}
// ----------------------------------------------------------------------------

void FrameworkTest::cacheInitialize(PropertiesPtr& props,
                                    const CacheAttributesPtr& cAttrs) {
#ifdef WIN32
  if (!m_doneSetNewAndDelete) {
    FWKEXCEPTION("SetNewAndDelete() not called via initArgs ctor");
  }
#endif

  CacheFactoryPtr cacheFactory;
  try {
    std::string name = getStringValue("systemName");
    bool isPdxSerialized = getBoolValue("PdxReadSerialized");
    if (name.empty()) {
      name = "TestDS";
    }
    bool isSslEnable = getBoolValue("sslEnable");
    if (isSslEnable) {
      props->insert("ssl-enabled", "true");
      std::string keystore =
          std::string(ACE_OS::getenv("BUILDDIR")) + "/framework/data/keystore";
      std::string pubkey = keystore + "/client_truststore.pem";
      std::string privkey = keystore + "/client_keystore.pem";
      props->insert("ssl-keystore", privkey.c_str());
      props->insert("ssl-truststore", pubkey.c_str());
    }
    cacheFactory = CacheFactory::createCacheFactory(props);

    if (isPdxSerialized) {
      cacheFactory->setPdxReadSerialized(isPdxSerialized);
    }
  } catch (Exception e) {
    FWKEXCEPTION(
        "DistributedSystem::connect encountered Exception: " << e.getMessage());
  }

  try {
    m_cache = cacheFactory->create();
    bool m_istransaction = getBoolValue("useTransactions");
    if (m_istransaction) {
      txManager = m_cache->getCacheTransactionManager();
    }
  } catch (CacheExistsException ignore) {
    m_cache = NULLPTR;
  } catch (Exception e) {
    FWKEXCEPTION(
        "CacheFactory::create encountered Exception: " << e.getMessage());
  }

  if (m_cache == NULLPTR) {
    FWKEXCEPTION("FrameworkTest: Failed to initialize cache.");
  }
}

// ----------------------------------------------------------------------------

void FrameworkTest::cacheFinalize() {
  if (m_cache != NULLPTR) {
    try {
      destroyAllRegions();
      m_cache->close();
    } catch (CacheClosedException ignore) {
    } catch (Exception& e) {
      FWKSEVERE("Caught an unexpected Exception during cache close: "
                << e.getMessage());
    } catch (...) {
      FWKSEVERE("Caught an unexpected unknown exception during cache close.");
    }
  }
  m_cache = NULLPTR;
  FWKINFO("Cache closed.");
}

// ----------------------------------------------------------------------------

void FrameworkTest::incClientCount() {
  char buf[16];
  sprintf(buf, "%d", m_id);
  std::string key(buf);
  int64_t cnt = m_bbc->increment(CLIENTBB, buf);
  int32_t scnt = static_cast<int32_t>(cnt);
  FWKINFO("Start count for client: " << m_id << " is currently: " << scnt);
  if (scnt == 1) {
    cnt = m_bbc->increment(CLIENTBB, CLIENTCOUNT);
    scnt = static_cast<int32_t>(cnt);
    FWKINFO("Client count is currently: " << scnt);
  }
}

// ----------------------------------------------------------------------------

void FrameworkTest::destroyAllRegions() {
  // destroy all root regions
  VectorOfRegion vec;
  m_cache->rootRegions(vec);
  int32_t size = vec.size();
  for (int32_t idx = 0; idx < size; idx++) {
    RegionPtr region = vec.at(idx);
    localDestroyRegion(region);
  }
}

// ----------------------------------------------------------------------------

void FrameworkTest::localDestroyRegion(RegionPtr& region) {
  try {
    region->localDestroyRegion();
  } catch (RegionDestroyedException& ignore) {
    ignore.getMessage();
    // the region could be already destroyed.
  } catch (Exception& ex) {
    FWKEXCEPTION("Caught unexpected exception during region local destroy: "
                 << ex.getMessage());
  }
}

void FrameworkTest::parseEndPoints(int32_t ep, std::string label,
                                   bool isServer) {
  std::string poolName = "_Test_Pool";
  PoolFactoryPtr pfPtr = PoolManager::createFactory();
  std::string tag = getStringValue("TAG");
  std::string bb("GFE_BB");

  std::string TAG("tags");
  std::string Count("count");
  std::string epList = " ";
  std::string stickykey("teststicky");
  std::string checksticky = bbGetString(bb, stickykey);

  bool multiUserMode = getBoolValue("multiUserMode");
  bool isExcpHandling = getBoolValue("isExcpHnd");
  FWKINFO("FrameworkTest::parseEndPoints ep count = : " << ep);
  for (int32_t i = 1; i <= ep; i++) {
    std::string key = label + "_";
    key.append(FwkStrCvt(i).toString());
    std::string ep = bbGetString(bb, key);
    epList += ep;
    size_t position = ep.find_first_of(":");
    if (position != std::string::npos) {
      std::string hostname = ep.substr(0, position);
      int portnumber = atoi((ep.substr(position + 1)).c_str());
      if (isServer) {
        pfPtr->addServer(hostname.c_str(), portnumber);
      } else {
        pfPtr->addLocator(hostname.c_str(), portnumber);
      }
    }
  }
  FWKINFO("Setting server or locator endpoints for pool:" << epList);
  FWKINFO("TESTSTICKY value is:" << checksticky);
  if ((checksticky.compare("ON")) == 0) {
    FWKINFO("setThreadLocalConnections to true & setMaxConnections to 13");
    pfPtr->setThreadLocalConnections(true);
    pfPtr->setPRSingleHopEnabled(false);
  } else {
    FWKINFO("ThreadLocalConnections set to false:Default");
  }

  if (isExcpHandling) {
    FWKINFO("The test is Exception Handling Test");
    pfPtr->setRetryAttempts(10);
  }

  if (multiUserMode) {
    FWKINFO("Setting multiuser mode");
    pfPtr->setMultiuserAuthentication(true);
    pfPtr->setSubscriptionEnabled(true);
  } else {
    pfPtr->setSubscriptionEnabled(true);
  }

  pfPtr->setMinConnections(20);
  pfPtr->setMaxConnections(30);
  pfPtr->setSubscriptionEnabled(true);
  pfPtr->setReadTimeout(180000);
  pfPtr->setFreeConnectionTimeout(180000);
  int32_t redundancyLevel = getIntValue("redundancyLevel");
  if (redundancyLevel > 0) pfPtr->setSubscriptionRedundancy(redundancyLevel);
  // create tag specific pools
  PoolPtr pptr = NULLPTR;
  if (!tag.empty()) {
    poolName.append(tag);
    // check if pool already exists
    pptr = PoolManager::find(poolName.c_str());
    if (pptr == NULLPTR) {
      pptr = pfPtr->create(poolName.c_str());
    }
  }
  // create default pool
  else {
    pptr = PoolManager::find(poolName.c_str());
    if (pptr == NULLPTR) {
      pptr = pfPtr->create(poolName.c_str());
    }
  }
  if (pptr != NULLPTR)
    FWKINFO(" Region Created with following Pool attributes :"
            << poolAttributesToString(pptr));
}

void FrameworkTest::createPool() {
  std::string bb("GFE_BB");
  std::string keys("testScheme");
  std::string mode = bbGetString(bb, keys);
  int32_t count = 0;
  std::string cnt;
  if (mode == "poolwithendpoints") {
    std::string label = "EndPoints";
    std::string tag = getStringValue("TAG");
    if (!tag.empty()) {
      label += "_";
      label += tag;
    }
    cnt = "EP_COUNT";
    count = static_cast<int32_t>(bbGet(bb, cnt));
    parseEndPoints(count, label, true);
  } else if (mode == "poolwithlocator") {
    std::string label = "LOCPORTS";
    cnt = "LOC_CNT";
    count = static_cast<int32_t>(bbGet(bb, cnt));
    parseEndPoints(count, label, false);
  }
}

QueryServicePtr FrameworkTest::checkQueryService() {
  PoolFactoryPtr pfPtr = PoolManager::createFactory();
  std::string bb("GFE_BB");
  std::string keys("testScheme");
  std::string mode = bbGetString(bb, keys);
  if (mode == "poolwithendpoints" || mode == "poolwithlocator") {
    PoolPtr pool = PoolManager::find("_Test_Pool");
    return pool->getQueryService();
  } else {
    return m_cache->getQueryService();
  }
}

void FrameworkTest::setTestScheme() {
  FWKINFO("FrameworkTest::setTestScheme called");
  std::string bb("GFE_BB");
  std::string key("testScheme");
  std::string lastpsc = bbGetString(bb, key);
  std::string psc = "";
  resetValue(key.c_str());
  while (psc != lastpsc) {
    psc = getStringValue(key.c_str());
  }
  while (psc == lastpsc || psc.empty()) {
    psc = getStringValue(key.c_str());
    if (psc.empty()) {
      break;
    }
  }
  if (!psc.empty()) {
    bbSet(bb, key, psc);
    FWKINFO("last test scheme = " << lastpsc << " current scheme = " << psc);
    FWKINFO("Test scheme : " << psc);
  }
}

std::string FrameworkTest::poolAttributesToString(PoolPtr& pool) {
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
  sString += FwkStrCvt(static_cast<int64_t>(pool->getIdleTimeout())).toString();
  sString += "\nPingInterval: ";
  sString +=
      FwkStrCvt(static_cast<int64_t>(pool->getPingInterval())).toString();
  sString += "\nThreadLocalConnections: ";
  sString += pool->getThreadLocalConnections() ? "true" : "false";
  sString += "\nMultiuserAuthentication: ";
  sString += pool->getMultiuserAuthentication() ? "true" : "false";
  sString += "\nPRSingleHopEnabled: ";
  sString += pool->getPRSingleHopEnabled() ? "true" : "false";
  sString += "\nLocator: ";
  CacheableStringArrayPtr str =
      dynamic_cast<CacheableStringArray*>(pool->getLocators().ptr());
  if (str != NULLPTR) {
    for (int32_t stri = 0; stri < str->length(); stri++) {
      sString += str->operator[](stri)->asChar();
      sString += ",";
    }
  }
  sString += "\nServers: ";
  str = dynamic_cast<CacheableStringArray*>(pool->getServers().ptr());
  if (str != NULLPTR) {
    for (int32_t stri = 0; stri < str->length(); stri++) {
      sString += str->operator[](stri)->asChar();
      sString += ",";
    }
  }
  sString += "\n";
  return sString;
}
