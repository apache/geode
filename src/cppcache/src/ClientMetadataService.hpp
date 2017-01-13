/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef CLIENT_METADATA_SERVICE
#define CLIENT_METADATA_SERVICE

#include <ace/Task.h>
#include "ClientMetadata.hpp"
#include "ServerLocation.hpp"
#include "BucketServerLocation.hpp"
#include <gfcpp/HashMapT.hpp>
#include <gfcpp/SharedPtr.hpp>
#include <gfcpp/CacheableKey.hpp>
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/Region.hpp>
#include "Queue.hpp"
#include <string>
#include "DistributedSystemImpl.hpp"
#include "NonCopyable.hpp"

namespace gemfire {

class ClienMetadata;

typedef std::map<std::string, ClientMetadataPtr> RegionMetadataMapType;

class BucketStatus {
 private:
  ACE_Time_Value m_lastTimeout;

 public:
  BucketStatus() : m_lastTimeout(ACE_Time_Value::zero) {}
  bool isTimedoutAndReset(uint32_t millis) {
    if (m_lastTimeout == ACE_Time_Value::zero) {
      return false;
    } else {
      ACE_Time_Value to(0, millis * 1000);
      to += m_lastTimeout;
      if (to > ACE_OS::gettimeofday())
        return true;  // timeout as buckste not recovered yet
      else {
        // reset to zero as we waited enough to recover bucket
        m_lastTimeout = ACE_Time_Value::zero;
        return false;
      }
    }
  }

  void setTimeout() {
    if (m_lastTimeout == ACE_Time_Value::zero)
      m_lastTimeout = ACE_OS::gettimeofday();  // set once only for timeout
  }
};

class PRbuckets {
 private:
  BucketStatus* m_buckets;

 public:
  PRbuckets(int32_t nBuckets) { m_buckets = new BucketStatus[nBuckets]; }
  ~PRbuckets() { delete[] m_buckets; }

  bool isBucketTimedOut(int32_t bucketId, uint32_t millis) {
    return m_buckets[bucketId].isTimedoutAndReset(millis);
  }

  void setBucketTimeout(int32_t bucketId) { m_buckets[bucketId].setTimeout(); }
};

/* adongre
 * CID 28726: Other violation (MISSING_COPY)
 * Class "gemfire::ClientMetadataService" owns resources that are managed
 * in its constructor and destructor but has no user-written copy constructor.
 *
 * CID 28712: Other violation (MISSING_ASSIGN)
 * Class "gemfire::ClientMetadataService" owns resources that are managed
 * in its constructor and destructor but has no user-written assignment
 * operator.
 *
 * FIX : Make the class NonCopyabl3
 */

class ClientMetadataService : public ACE_Task_Base,
                              private NonCopyable,
                              private NonAssignable {
 public:
  ~ClientMetadataService();
  ClientMetadataService(PoolPtr pool);

  inline void start() {
    m_run = true;
    this->activate();
  }

  inline void stop() {
    m_run = false;
    m_regionQueueSema.release();
    this->wait();
  }

  int svc(void);

  void getClientPRMetadata(const char* regionFullPath);

  void getBucketServerLocation(
      const RegionPtr& region, const CacheableKeyPtr& key,
      const CacheablePtr& value, const UserDataPtr& aCallbackArgument,
      bool isPrimary, BucketServerLocationPtr& serverLocation, int8_t& version);

  void removeBucketServerLocation(BucketServerLocation serverLocation);

  ClientMetadataPtr getClientMetadata(const char* regionFullPath);

  void populateDummyServers(const char* regionName,
                            ClientMetadataPtr clientmetadata);

  void enqueueForMetadataRefresh(const char* regionFullPath,
                                 int8 serverGroupFlag);

  HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>*
  getServerToFilterMap(const VectorOfCacheableKey* keys,
                       const RegionPtr& region, bool isPrimary);

  void markPrimaryBucketForTimeout(
      const RegionPtr& region, const CacheableKeyPtr& key,
      const CacheablePtr& value, const UserDataPtr& aCallbackArgument,
      bool isPrimary, BucketServerLocationPtr& serverLocation, int8_t& version);

  void markPrimaryBucketForTimeoutButLookSecondaryBucket(
      const RegionPtr& region, const CacheableKeyPtr& key,
      const CacheablePtr& value, const UserDataPtr& aCallbackArgument,
      bool isPrimary, BucketServerLocationPtr& serverLocation, int8_t& version);

  bool isBucketMarkedForTimeout(const char* regionFullPath, int32_t bucketid);

  bool AreBucketSetsEqual(CacheableHashSetPtr& currentBucketSet,
                          CacheableHashSetPtr& bucketSet);

  BucketServerLocationPtr findNextServer(
      HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>*
          serverToBucketsMap,
      CacheableHashSetPtr& currentBucketSet);

  HashMapT<CacheableInt32Ptr, CacheableHashSetPtr>* groupByBucketOnClientSide(
      const RegionPtr& region, CacheableVectorPtr* keySet,
      ClientMetadataPtr& metadata);

  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>*
  getServerToFilterMapFESHOP(CacheableVectorPtr* keySet,
                             const RegionPtr& region, bool isPrimary);

  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>*
  groupByServerToAllBuckets(const RegionPtr& region, bool optimizeForWrite);

  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>*
  groupByServerToBuckets(ClientMetadataPtr& metadata,
                         CacheableHashSetPtr& bucketSet, bool optimizeForWrite);

  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* pruneNodes(
      ClientMetadataPtr& metadata, CacheableHashSetPtr& totalBuckets);

 private:
  // const PartitionResolverPtr& getResolver(const RegionPtr& region, const
  // CacheableKeyPtr& key,
  // const UserDataPtr& aCallbackArgument);

  // BucketServerLocation getServerLocation(ClientMetadataPtr cptr, int
  // bucketId, bool isPrimary);

  ClientMetadataPtr SendClientPRMetadata(const char* regionPath,
                                         ClientMetadataPtr cptr);

 private:
  // ACE_Recursive_Thread_Mutex m_regionMetadataLock;
  ACE_RW_Thread_Mutex m_regionMetadataLock;
  ClientMetadataService();
  ACE_Semaphore m_regionQueueSema;
  RegionMetadataMapType m_regionMetaDataMap;
  volatile bool m_run;
  PoolPtr m_pool;
  Queue<std::string>* m_regionQueue;

  ACE_RW_Thread_Mutex m_PRbucketStatusLock;
  std::map<std::string, PRbuckets*> m_bucketStatus;
  uint32_t m_bucketWaitTimeout;
  static const char* NC_CMDSvcThread;
};
}

#endif
