#pragma once

#ifndef GEODE_CLIENTMETADATASERVICE_H_
#define GEODE_CLIENTMETADATASERVICE_H_

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

namespace apache {
namespace geode {
namespace client {

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
      if (to > ACE_OS::gettimeofday()) {
        return true;  // timeout as buckste not recovered yet
      } else {
        // reset to zero as we waited enough to recover bucket
        m_lastTimeout = ACE_Time_Value::zero;
        return false;
      }
    }
  }

  void setTimeout() {
    if (m_lastTimeout == ACE_Time_Value::zero) {
      m_lastTimeout = ACE_OS::gettimeofday();  // set once only for timeout
    }
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
 * Class "apache::geode::client::ClientMetadataService" owns resources that are
 * managed
 * in its constructor and destructor but has no user-written copy constructor.
 *
 * CID 28712: Other violation (MISSING_ASSIGN)
 * Class "apache::geode::client::ClientMetadataService" owns resources that are
 * managed
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
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_CLIENTMETADATASERVICE_H_
