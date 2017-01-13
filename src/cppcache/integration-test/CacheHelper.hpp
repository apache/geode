/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef TEST_CACHEHELPER_HPP
#define TEST_CACHEHELPER_HPP

#include <gfcpp/GemfireCppCache.hpp>
#include <stdlib.h>
#include <gfcpp/SystemProperties.hpp>
#include <ace/OS.h>
#include <ace/INET_Addr.h>
#include <ace/SOCK_Acceptor.h>

#include "TimeBomb.hpp"
#include <list>
#include <chrono>
#include "DistributedSystemImpl.hpp"
#include "Utils.hpp"
#include <gfcpp/PoolManager.hpp>
#ifndef ROOT_NAME
#define ROOT_NAME "Root"
#endif

#ifndef ROOT_SCOPE
#define ROOT_SCOPE LOCAL
#endif

using namespace gemfire;

class CacheHelper {
 public:
  static CacheHelper* singleton;
  static std::list<std::string> staticConfigFileList;
  CachePtr cachePtr;
  RegionPtr rootRegionPtr;
  bool m_doDisconnect;

  CachePtr getCache();

  static CacheHelper& getHelper();

  static PoolPtr getPoolPtr(const char* poolName);
  static std::string unitTestOutputFile();
  static int getNumLocatorListUpdates(const char* s);

  CacheHelper(const char* member_id, const PropertiesPtr& configPtr = NULLPTR,
              const bool noRootRegion = false);

  /** rootRegionPtr will still be null... */
  CacheHelper(const char* member_id, const char* cachexml,
              const PropertiesPtr& configPtr = NULLPTR);

  CacheHelper(const PropertiesPtr& configPtr = NULLPTR,
              const bool noRootRegion = false);

  CacheHelper(const bool isThinclient, const PropertiesPtr& configPtr = NULLPTR,
              const bool noRootRegion = false);

  CacheHelper(const bool isThinclient, bool pdxIgnoreUnreadFields,
              bool pdxReadSerialized, const PropertiesPtr& configPtr = NULLPTR,
              const bool noRootRegion = false);

  CacheHelper(const bool isthinClient, const char* poolName,
              const char* locators, const char* serverGroup,
              const PropertiesPtr& configPtr = NULLPTR, int redundancy = 0,
              bool clientNotification = false, int subscriptionAckInterval = -1,
              int connections = -1, int loadConditioningInterval = -1,
              bool isMultiuserMode = false, bool prSingleHop = false,
              bool threadLocal = false);

  CacheHelper(const int redundancyLevel,
              const PropertiesPtr& configPtr = NULLPTR);

  virtual ~CacheHelper();

  void closePool(const char* poolName, bool keepAlive = false);

  void disconnect(bool keepalive = false);

  void createPlainRegion(const char* regionName, RegionPtr& regionPtr);

  void createPlainRegion(const char* regionName, RegionPtr& regionPtr,
                         uint32_t size);

  void createLRURegion(const char* regionName, RegionPtr& regionPtr);

  void createLRURegion(const char* regionName, RegionPtr& regionPtr,
                       uint32_t size);

  void createDistRegion(const char* regionName, RegionPtr& regionPtr);

  void createDistRegion(const char* regionName, RegionPtr& regionPtr,
                        uint32_t size);

  RegionPtr getRegion(const char* name);

  RegionPtr createRegion(const char* name, bool ack, bool caching,
                         const CacheListenerPtr& listener,
                         bool clientNotificationEnabled = false,
                         bool scopeLocal = false,
                         bool concurrencyCheckEnabled = false,
                         int32_t tombstonetimeout = -1);

  RegionPtr createRegion(
      const char* name, bool ack, bool caching = true, int ettl = 0,
      int eit = 0, int rttl = 0, int rit = 0, int lel = 0,
      ExpirationAction::Action action = ExpirationAction::DESTROY,
      const char* endpoints = 0, bool clientNotificationEnabled = false);

  PoolPtr createPool(const char* poolName, const char* locators,
                     const char* serverGroup, int redundancy = 0,
                     bool clientNotification = false,
                     int subscriptionAckInterval = -1, int connections = -1,
                     int loadConditioningInterval = -1,
                     bool isMultiuserMode = false);

  // this will create pool even endpoints and locatorhost has been not defined
  PoolPtr createPool2(const char* poolName, const char* locators,
                      const char* serverGroup, const char* servers = NULL,
                      int redundancy = 0, bool clientNotification = false,
                      int subscriptionAckInterval = -1, int connections = -1);

  void logPoolAttributes(PoolPtr& pool);

  void createPoolWithLocators(const char* name, const char* locators = NULL,
                              bool clientNotificationEnabled = false,
                              int subscriptionRedundancy = -1,
                              int subscriptionAckInterval = -1,
                              int connections = -1,
                              bool isMultiuserMode = false,
                              const char* serverGroup = NULL);

  RegionPtr createRegionAndAttachPool(
      const char* name, bool ack, const char* poolName = NULL,
      bool caching = true, int ettl = 0, int eit = 0, int rttl = 0, int rit = 0,
      int lel = 0, ExpirationAction::Action action = ExpirationAction::DESTROY);

  RegionPtr createRegionAndAttachPool2(
      const char* name, bool ack, const char* poolName,
      const PartitionResolverPtr& aResolver = NULLPTR, bool caching = true,
      int ettl = 0, int eit = 0, int rttl = 0, int rit = 0, int lel = 0,
      ExpirationAction::Action action = ExpirationAction::DESTROY);

  void addServerLocatorEPs(const char* epList, PoolFactoryPtr pfPtr,
                           bool poolLocators = true);

  void addServerLocatorEPs(const char* epList, CacheFactoryPtr cacheFac,
                           bool poolLocators = true);

  RegionPtr createPooledRegion(
      const char* name, bool ack, const char* locators = 0,
      const char* poolName = "__TEST_POOL1__", bool caching = true,
      bool clientNotificationEnabled = false, int ettl = 0, int eit = 0,
      int rttl = 0, int rit = 0, int lel = 0,
      const CacheListenerPtr& cacheListener = NULLPTR,
      ExpirationAction::Action action = ExpirationAction::DESTROY);

  RegionPtr createPooledRegionConcurrencyCheckDisabled(
      const char* name, bool ack, const char* locators = 0,
      const char* poolName = "__TEST_POOL1__", bool caching = true,
      bool clientNotificationEnabled = false,
      bool concurrencyCheckEnabled = true, int ettl = 0, int eit = 0,
      int rttl = 0, int rit = 0, int lel = 0,
      const CacheListenerPtr& cacheListener = NULLPTR,
      ExpirationAction::Action action = ExpirationAction::DESTROY);

  RegionPtr createRegionDiscOverFlow(
      const char* name, bool caching = true,
      bool clientNotificationEnabled = false, int ettl = 0, int eit = 0,
      int rttl = 0, int rit = 0, int lel = 0,
      ExpirationAction::Action action = ExpirationAction::DESTROY);

  RegionPtr createPooledRegionDiscOverFlow(
      const char* name, bool ack, const char* locators = 0,
      const char* poolName = "__TEST_POOL1__", bool caching = true,
      bool clientNotificationEnabled = false, int ettl = 0, int eit = 0,
      int rttl = 0, int rit = 0, int lel = 0,
      const CacheListenerPtr& cacheListener = NULLPTR,
      ExpirationAction::Action action = ExpirationAction::DESTROY);

  RegionPtr createPooledRegionSticky(
      const char* name, bool ack, const char* locators = 0,
      const char* poolName = "__TEST_POOL1__", bool caching = true,
      bool clientNotificationEnabled = false, int ettl = 0, int eit = 0,
      int rttl = 0, int rit = 0, int lel = 0,
      const CacheListenerPtr& cacheListener = NULLPTR,
      ExpirationAction::Action action = ExpirationAction::DESTROY);

  RegionPtr createPooledRegionStickySingleHop(
      const char* name, bool ack, const char* locators = 0,
      const char* poolName = "__TEST_POOL1__", bool caching = true,
      bool clientNotificationEnabled = false, int ettl = 0, int eit = 0,
      int rttl = 0, int rit = 0, int lel = 0,
      const CacheListenerPtr& cacheListener = NULLPTR,
      ExpirationAction::Action action = ExpirationAction::DESTROY);

  RegionPtr createSubregion(RegionPtr& parent, const char* name, bool ack,
                            bool caching, const CacheListenerPtr& listener);

  CacheableStringPtr createCacheable(const char* value);

  void showKeys(VectorOfCacheableKey& vecKeys);

  void showRegionAttributes(RegionAttributes& attributes);

  QueryServicePtr getQueryService();

  /*
   * GFJAVA is the environment variable. user has to set GFJAVA variable as a
   * product build directory
   * path for java cache server or set as endpoints list for the remote server
   */

  static int staticHostPort1;
  static int staticHostPort2;
  static int staticHostPort3;
  static int staticHostPort4;

  static const char* getTcrEndpoints(bool& isLocalServer,
                                     int numberOfServers = 1);

  static int staticLocatorHostPort1;
  static int staticLocatorHostPort2;
  static int staticLocatorHostPort3;
  static const char* getstaticLocatorHostPort1();

  static const char* getstaticLocatorHostPort2();

  static const char* getLocatorHostPort(int locPort);

  static const char* getLocatorHostPort(bool& isLocator, bool& isLocalServer,
                                        int numberOfLocators = 0);

  static const char* getTcrEndpoints2(bool& isLocalServer,
                                      int numberOfServers = 1);

  static std::list<int> staticServerInstanceList;
  static bool isServerCleanupCallbackRegistered;
  static void cleanupServerInstances();

  static void initServer(int instance, const char* xml = NULL,
                         const char* locHostport = NULL,
                         const char* authParam = NULL, bool ssl = false,
                         bool enableDelta = true, bool multiDS = false,
                         bool testServerGC = false);

  static void createDuplicateXMLFile(std::string& originalFile, int hostport1,
                                     int hostport2, int locport1, int locport2);

  static void createDuplicateXMLFile(std::string& duplicateFile,
                                     std::string& originalFile);

  static void closeServer(int instance);

  // closing locator
  static void closeLocator(int instance, bool ssl = false);
  template <class Rep, class Period>
  static void terminate_process_file(
      const std::string& pidFileName,
      const std::chrono::duration<Rep, Period>& duration);
  static bool file_exists(const std::string& fileName);
  static void read_single_line(const std::string& fileName, std::string& str);

  static void cleanupTmpConfigFiles();

  static std::list<int> staticLocatorInstanceList;
  static bool isLocatorCleanupCallbackRegistered;
  static void cleanupLocatorInstances();

  // starting locator
  static void initLocator(int instance, bool ssl = false, bool multiDS = false,
                          int dsId = -1, int remoteLocator = 0);

  static void clearSecProp();

  static void setJavaConnectionPoolSize(long size);

  static bool isSeedSet;
  static bool setSeed();

  static int hashcode(char* str);

  static int getRandomNumber();

  static int getRandomAvailablePort();

  static int staticMcastPort;
  static int staticMcastAddress;

 private:
  static std::string generateGemfireProperties(const std::string& path,
                                               const bool ssl = false,
                                               const int dsId = -1,
                                               const int remoteLocator = 0);
};

#ifndef test_cppcache_utils_static
CacheHelper* CacheHelper::singleton = NULL;
std::list<int> CacheHelper::staticServerInstanceList;
std::list<int> CacheHelper::staticLocatorInstanceList;
std::list<std::string> CacheHelper::staticConfigFileList;
bool CacheHelper::isServerCleanupCallbackRegistered = false;
bool CacheHelper::isLocatorCleanupCallbackRegistered = false;

bool CacheHelper::isSeedSet = CacheHelper::setSeed();
int CacheHelper::staticMcastAddress = CacheHelper::getRandomNumber() % 250 + 3;
int CacheHelper::staticMcastPort = CacheHelper::getRandomNumber();
int CacheHelper::staticHostPort1 = CacheHelper::getRandomAvailablePort();
int CacheHelper::staticHostPort2 = CacheHelper::getRandomAvailablePort();
int CacheHelper::staticHostPort3 = CacheHelper::getRandomAvailablePort();
int CacheHelper::staticHostPort4 = CacheHelper::getRandomAvailablePort();

int CacheHelper::staticLocatorHostPort1 = CacheHelper::getRandomAvailablePort();
int CacheHelper::staticLocatorHostPort2 = CacheHelper::getRandomAvailablePort();
int CacheHelper::staticLocatorHostPort3 = CacheHelper::getRandomAvailablePort();
#endif
#endif  // TEST_CACHEHELPER_HPP
