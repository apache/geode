#pragma once

#ifndef GEODE_CLIENTMETADATA_H_
#define GEODE_CLIENTMETADATA_H_

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

#include <gfcpp/PartitionResolver.hpp>
#include "ServerLocation.hpp"
#include "BucketServerLocation.hpp"
#include "ReadWriteLock.hpp"
#include "FixedPartitionAttributesImpl.hpp"
#include <ace/ACE.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <vector>
#include <map>
#include "NonCopyable.hpp"

/*Stores the information such as partition attributes and meta data details*/

namespace apache {
namespace geode {
namespace client {
class ThinClientPoolDM;
class ClientMetadata;

typedef SharedPtr<ClientMetadata> ClientMetadataPtr;
typedef std::vector<BucketServerLocationPtr> BucketServerLocationsType;
// typedef std::map<int,BucketServerLocationsType >
// BucketServerLocationsListType;
typedef std::vector<BucketServerLocationsType> BucketServerLocationsListType;
typedef std::map<std::string, std::vector<int> > FixedMapType;

class CPPCACHE_EXPORT ClientMetadata : public SharedBase, public NonAssignable {
 private:
  void setPartitionNames();
  CacheableHashSetPtr m_partitionNames;

  BucketServerLocationsListType m_bucketServerLocationsList;
  ClientMetadataPtr m_previousOne;
  int m_totalNumBuckets;
  // PartitionResolverPtr m_partitionResolver;
  CacheableStringPtr m_colocatedWith;
  // ACE_RW_Thread_Mutex m_readWriteLock;
  ThinClientPoolDM* m_tcrdm;
  FixedMapType m_fpaMap;
  inline void checkBucketId(size_t bucketId) {
    if (bucketId >= m_bucketServerLocationsList.size()) {
      LOGERROR("ClientMetadata::getServerLocation(): BucketId out of range.");
      throw IllegalStateException(
          "ClientMetadata::getServerLocation(): BucketId out of range.");
    }
  }

 public:
  void setPreviousone(ClientMetadataPtr cptr) { m_previousOne = cptr; }
  ~ClientMetadata();
  ClientMetadata();
  ClientMetadata(int totalNumBuckets, CacheableStringPtr colocatedWith,
                 ThinClientPoolDM* tcrdm,
                 std::vector<FixedPartitionAttributesImplPtr>* fpaSet);
  void getServerLocation(int bucketId, bool tryPrimary,
                         BucketServerLocationPtr& serverLocation,
                         int8_t& version);
  // ServerLocation getPrimaryServerLocation(int bucketId);
  void updateBucketServerLocations(
      int bucketId, BucketServerLocationsType bucketServerLocations);
  void removeBucketServerLocation(BucketServerLocation serverLocation);
  int getTotalNumBuckets();
  // PartitionResolverPtr getPartitionResolver();
  CacheableStringPtr getColocatedWith();
  void populateDummyServers(int bucketId, BucketServerLocationsType serverlist);
  int assignFixedBucketId(const char* partitionName,
                          CacheableKeyPtr resolvekey);
  CacheableHashSetPtr& getFixedPartitionNames() {
    /* if(m_fpaMap.size() >0)
     {
       CacheableHashSetPtr partitionNames = CacheableHashSet::create();
       for ( FixedMapType::iterator it=m_fpaMap.begin() ; it != m_fpaMap.end();
     it++ ) {
         partitionNames->insert(CacheableString::create(((*it).first).c_str()));
       }
       return partitionNames;
     }*/
    return m_partitionNames;
  }
  ClientMetadata(ClientMetadata& other);
  std::vector<BucketServerLocationPtr> adviseServerLocations(int bucketId);
  BucketServerLocationPtr advisePrimaryServerLocation(int bucketId);
  BucketServerLocationPtr adviseRandomServerLocation();
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_CLIENTMETADATA_H_
