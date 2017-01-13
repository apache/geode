#ifndef __GEMFIRE_CACHEIMPL_H__
#define __GEMFIRE_CACHEIMPL_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>

#include <gfcpp/Cache.hpp>
#include <gfcpp/CacheAttributes.hpp>
#include <gfcpp/DistributedSystem.hpp>
#include "MapWithLock.hpp"
#include "SpinLock.hpp"
#include <ace/ACE.h>
#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>
#include <ace/Recursive_Thread_Mutex.h>
#include "Condition.hpp"
#include "TcrConnectionManager.hpp"
#include "EvictionController.hpp"
#include "RemoteQueryService.hpp"
#include "AdminRegion.hpp"
#include "CachePerfStats.hpp"
#include "PdxTypeRegistry.hpp"
#include "MemberListForVersionStamp.hpp"

#include <string.h>
#include <string>
#include <map>

#include "NonCopyable.hpp"

/** @todo period '.' consistency */
/** @todo fix returns to param documentation of result ptr... */

/**
 * @file
 */

namespace gemfire {

class CacheFactory;
class ExpiryTaskManager;

/**
 * @class Cache Cache.hpp
 * GemFire's implementation of a distributed C++ Cache.
 *
 * Caches are obtained from static methods on the {@link CacheFactory} class.
 * <p>
 * When a cache is created a {@link DistributedSystem} must be specified.
 * This system tells the cache where to find other caches on the network
 * and how to communicate with them.
 * <p>
 * When a cache will no longer be used, it should be {@link #close closed}.
 * Once it {@link Cache::isClosed is closed} any attempt to use it

 * will cause a <code>CacheClosedException</code> to be thrown.
 *
 * <p>A cache can have multiple root regions, each with a different name.
 *
 */

/* adongre
 * CID 28711: Other violation (MISSING_ASSIGN)
 * Class "gemfire::CacheImpl" owns resources that are managed
 * in its constructor and destructor but has no user-written assignment
 * operator.
 *
 * Fix : Make the class Non copyable and non assignable
 */
class CPPCACHE_EXPORT CacheImpl : private NonCopyable, private NonAssignable {
  /**
   * @brief public methods
   */
 public:
  // added netDown and revive for tests to simulate client crash and network
  // drop
  void netDown();
  void revive();
  void setClientCrashTEST() { m_tcrConnectionManager->setClientCrashTEST(); }

  // For PrSingleHop C++unit testing.
  static ACE_Recursive_Thread_Mutex s_nwHopLock;
  static void setNetworkHopFlag(bool networkhopflag) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _lock(s_nwHopLock);
    CacheImpl::s_networkhop = networkhopflag;
  }
  static bool getAndResetNetworkHopFlag();

  static int blackListBucketTimeouts();
  static void setBlackListBucketTimeouts();

  static void setServerGroupFlag(int8 serverGroupFlag) {
    CacheImpl::s_serverGroupFlag = serverGroupFlag;
  }
  static int8 getAndResetServerGroupFlag();
  static MemberListForVersionStampPtr getMemberListForVersionStamp();

  /** Returns the name of this cache.
   * @return the string name of this cache
   */
  const char* getName() const;

  /**
   * Indicates if this cache has been closed.
   * After a new cache object is created, this method returns false;
   * After the close is called on this cache object, this method
   * returns true.
   *
   * @return true, if this cache is closed; false, otherwise
   */
  bool isClosed() const;

  /** Get the <code>CacheAttributes</code> for this cache. */
  inline CacheAttributesPtr getAttributes() const { return m_attributes; }

  /** Set the <code>CacheAttributes</code> for this cache. */
  void setAttributes(const CacheAttributesPtr& attrs);

  /**
   * Returns the distributed system that this cache was
   * {@link CacheFactory::create created} with.
   */
  void getDistributedSystem(DistributedSystemPtr& dptr) const;

  /**
   * Terminates this object cache and releases all the local resources.
   * After this cache is closed, any further
   * method call on this cache or any region object will throw
   * <code>CacheClosedException</code>, unless otherwise noted.
   * @param keepalive whether to keep a durable client's queue alive.
   * @throws CacheClosedException,  if the cache is already closed.
   */
  void close(bool keepalive = false);

  /**
   * Creates a region  using the specified
   * RegionAttributes.
   *
   * @param name the name of the region to create
   * @param aRegionAttributes the attributes of the root region
   * @todo change return to param for regionPtr...
   * @param regionPtr the pointer object pointing to the returned region object
   * when the function returns
   * @throws InvalidArgumentException if the attributePtr is NULL.
   * @throws RegionExistsException if a region is already in
   * this cache
   * @throws CacheClosedException if the cache is closed
   * @throws OutOfMemoryException if the memory allocation failed
   * @throws NotConnectedException if the cache is not connected
   * @throws UnknownException otherwise
   */
  void createRegion(const char* name,
                    const RegionAttributesPtr& aRegionAttributes,
                    RegionPtr& regionPtr);

  void getRegion(const char* path, RegionPtr& rptr);

  /**
   * Returns a set of root regions in the cache. Does not cause any
   * shared regions to be mapped into the cache. This set is a snapshot and
   * is not backed by the Cache. The regions passed in are cleared.
   *
   * @param regions the region collection object containing the returned set of
   * regions when the function returns
   */

  void rootRegions(VectorOfRegion& regions);

  /**
   * FUTURE: not used currently. Gets the number of seconds a cache
   * {@link Region::get} operation
   * can spend searching for a value before it times out.
   * The search includes any time spent loading the object.
   * When the search times out, it causes the get to fail by throwing
   * an exception.
   * This method does not throw
   * <code>CacheClosedException</code> if the cache is closed.
   * Sets the number of seconds a cache get operation can spend searching
   * for a value.
   *
   * @throws IllegalArgumentException if <code>seconds</code> is less than zero
   */
  inline void setSearchTimeout(int seconds = 0) {}

  virtual RegionFactoryPtr createRegionFactory(RegionShortcut preDefinedRegion);

  CacheTransactionManagerPtr getCacheTransactionManager();

  /**
    * @brief destructor
    */
  virtual ~CacheImpl();
  /**
   * @brief constructors
   */
  CacheImpl(Cache* c, const char* name, DistributedSystemPtr sys,
            bool ignorePdxUnreadFields, bool readPdxSerialized);
  CacheImpl(Cache* c, const char* name, DistributedSystemPtr sys,
            const char* id_data, bool ignorePdxUnreadFields,
            bool readPdxSerialized);
  void initServices();
  EvictionController* getEvictionController();

  static ExpiryTaskManager* expiryTaskManager;
  Cache* getCache() const { return m_implementee; }
  TcrConnectionManager& tcrConnectionManager() {
    return *m_tcrConnectionManager;
  }

  int removeRegion(const char* name);

  QueryServicePtr getQueryService(bool noInit = false);

  QueryServicePtr getQueryService(const char* poolName);

  RegionInternal* createRegion_internal(const std::string& name,
                                        RegionInternal* rootRegion,
                                        const RegionAttributesPtr& attrs,
                                        const CacheStatisticsPtr& csptr,
                                        bool shared);

  /**
   * Send the "client ready" message to the server.
   */
  void readyForEvents();

  //  TESTING: Durable clients. Not thread safe.
  bool getEndpointStatus(const std::string& endpoint);

  void processMarker();

  // Version ordinal accessors for unit tests
  static void setVersionOrdinalForTest(int8_t newVer);
  static int8_t getVersionOrdinalForTest();

  // Pool helpers for unit tests
  static int getPoolSize(const char* poolName);

  // CachePerfStats
  CachePerfStats* m_cacheStats;

  static inline CacheImpl* getInstance() { return s_instance; };

  bool getCacheMode() {
    return m_attributes == NULLPTR ? false : m_attributes->m_cacheMode;
  }

  bool getPdxIgnoreUnreadFields() { return m_ignorePdxUnreadFields; }

  void setPdxIgnoreUnreadFields(bool ignore) {
    m_ignorePdxUnreadFields = ignore;
  }

  void setPdxReadSerialized(bool val) { m_readPdxSerialized = val; }
  bool getPdxReadSerialized() { return m_readPdxSerialized; }
  bool isCacheDestroyPending() const;

  void setDefaultPool(PoolPtr pool);

  PoolPtr getDefaultPool();

  static void setRegionShortcut(AttributesFactoryPtr attrFact,
                                RegionShortcut preDefinedRegionAttr);

  static std::map<std::string, RegionAttributesPtr> getRegionShortcut();

 private:
  static volatile bool s_networkhop;
  static volatile int s_blacklistBucketTimeout;
  static volatile int8 s_serverGroupFlag;
  static MemberListForVersionStampPtr s_versionStampMemIdList;
  PoolPtr m_defaultPool;
  bool m_ignorePdxUnreadFields;
  bool m_readPdxSerialized;

  enum RegionKind {
    CPP_REGION,
    THINCLIENT_REGION,
    THINCLIENT_HA_REGION,
    THINCLIENT_POOL_REGION
  };

  RegionKind getRegionKind(const RegionAttributesPtr& rattrs) const;

  void sendNotificationCloseMsgs();

  void validateRegionAttributes(const char* name,
                                const RegionAttributesPtr& attrs) const;

  inline void getSubRegions(MapOfRegionWithLock& srm) {
    MapOfRegionGuard guard(m_regions->mutex());
    if (m_regions->current_size() == 0) return;
    for (MapOfRegionWithLock::iterator p = m_regions->begin();
         p != m_regions->end(); ++p) {
      srm.bind((*p).ext_id_, (*p).int_id_);
    }
  }
  char* m_name;
  bool m_closed;
  bool m_initialized;

  DistributedSystemPtr m_distributedSystem;
  MapOfRegionWithLock* m_regions;
  Cache* m_implementee;
  ACE_Recursive_Thread_Mutex m_mutex;
  Condition m_cond;
  CacheAttributesPtr m_attributes;
  EvictionController* m_evictionControllerPtr;
  TcrConnectionManager* m_tcrConnectionManager;
  RemoteQueryServicePtr m_remoteQueryServicePtr;
  ACE_RW_Thread_Mutex m_destroyCacheMutex;
  volatile bool m_destroyPending;
  volatile bool m_initDone;
  static CacheImpl* s_instance;
  ACE_Thread_Mutex m_initDoneLock;
  AdminRegionPtr m_adminRegion;
  CacheTransactionManagerPtr m_cacheTXManager;

  friend class CacheFactory;
  friend class Cache;
};

};      // namespace gemfire
#endif  // ifndef __GEMFIRE_CACHEIMPL_H__
