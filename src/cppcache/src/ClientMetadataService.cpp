/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "TcrMessage.hpp"
#include "ClientMetadataService.hpp"
#include "ThinClientPoolDM.hpp"
#include <gfcpp/FixedPartitionResolver.hpp>
#include <iterator>
#include <cstdlib>
#include <climits>

namespace gemfire {
const char* ClientMetadataService::NC_CMDSvcThread = "NC CMDSvcThread";
ClientMetadataService::~ClientMetadataService() {
  delete m_regionQueue;
  if (m_bucketWaitTimeout > 0) {
    try {
      std::map<std::string, PRbuckets*>::iterator bi;
      for (bi = m_bucketStatus.begin(); bi != m_bucketStatus.end(); ++bi) {
        delete bi->second;
      }

    } catch (...) {
      LOGINFO("Exception in ClientMetadataService destructor");
    }
  }
}

ClientMetadataService::ClientMetadataService(PoolPtr pool)
    /* adongre
    * CID 28928: Uninitialized scalar field (UNINIT_CTOR)
    */
    : m_run(false)

{
  m_regionQueue = new Queue<std::string>(false);
  m_pool = pool;
  m_bucketWaitTimeout =
      DistributedSystem::getSystemProperties()->bucketWaitTimeout();
}

int ClientMetadataService::svc() {
  DistributedSystemImpl::setThreadName(NC_CMDSvcThread);
  LOGINFO("ClientMetadataService started for pool %s", m_pool->getName());
  while (m_run) {
    m_regionQueueSema.acquire();
    ThinClientPoolDM* tcrdm = dynamic_cast<ThinClientPoolDM*>(m_pool.ptr());
    CacheImpl* cache = tcrdm->getConnectionManager().getCacheImpl();
    while (true) {
      std::string* regionFullPath = m_regionQueue->get();

      if (regionFullPath != NULL && regionFullPath->c_str() != NULL) {
        while (true) {
          if (m_regionQueue->size() > 0) {
            std::string* nextRegionFullPath = m_regionQueue->get();
            if (nextRegionFullPath != NULL &&
                nextRegionFullPath->c_str() != NULL &&
                regionFullPath->compare(nextRegionFullPath->c_str()) == 0) {
              delete nextRegionFullPath;  // we are going for same
            } else {
              // different region; put it back
              m_regionQueue->put(nextRegionFullPath);
              break;
            }
          } else {
            break;
          }
        }
      }

      if (!cache->isCacheDestroyPending() && regionFullPath != NULL &&
          regionFullPath->c_str() != NULL) {
        getClientPRMetadata(regionFullPath->c_str());
        delete regionFullPath;
        regionFullPath = NULL;
      } else {
        delete regionFullPath;
        regionFullPath = NULL;
        break;
      }
    }
    // while(m_regionQueueSema.tryacquire( ) != -1); // release all
  }
  LOGINFO("ClientMetadataService stopped for pool %s", m_pool->getName());
  return 0;
}

void ClientMetadataService::getClientPRMetadata(const char* regionFullPath) {
  if (regionFullPath == NULL) return;
  ThinClientPoolDM* tcrdm = dynamic_cast<ThinClientPoolDM*>(m_pool.ptr());
  if (tcrdm == NULL) {
    throw IllegalArgumentException(
        "ClientMetaData: pool cast to ThinClientPoolDM failed");
  }
  // That means metadata for the region not found, So only for the first time
  // for a particular region use GetClientPartitionAttributesOp
  // TcrMessage to fetch the metadata and put it into map for later use.send
  // this message to server and get metadata from server.
  TcrMessageReply reply(true, NULL);
  std::string path(regionFullPath);
  ClientMetadataPtr cptr = NULLPTR;
  {
    ReadGuard guard(m_regionMetadataLock);
    RegionMetadataMapType::iterator itr = m_regionMetaDataMap.find(path);
    if (itr != m_regionMetaDataMap.end()) {
      cptr = itr->second;
    }
    // cptr = m_regionMetaDataMap[path];
  }
  ClientMetadataPtr newCptr = NULLPTR;

  {
    // ACE_Guard< ACE_Recursive_Thread_Mutex > guard( m_regionMetadataLock );

    if (cptr == NULLPTR) {
      TcrMessageGetClientPartitionAttributes request(regionFullPath);
      GfErrType err = tcrdm->sendSyncRequest(request, reply);
      if (err == GF_NOERR &&
          reply.getMessageType() ==
              TcrMessage::RESPONSE_CLIENT_PARTITION_ATTRIBUTES) {
        cptr =
            new ClientMetadata(reply.getNumBuckets(), reply.getColocatedWith(),
                               tcrdm, reply.getFpaSet());
        if (m_bucketWaitTimeout > 0 && reply.getNumBuckets() > 0) {
          WriteGuard guard(m_PRbucketStatusLock);
          m_bucketStatus[regionFullPath] = new PRbuckets(reply.getNumBuckets());
        }
        LOGDEBUG("ClientMetadata buckets %d ", reply.getNumBuckets());
        if (cptr != NULLPTR) {
          // m_regionMetaDataMap[regionFullPath] = cptr;
        }
      }
    }
  }
  if (cptr == NULLPTR) {
    return;
  }
  CacheableStringPtr colocatedWith;
  if (cptr != NULLPTR) {
    colocatedWith = cptr->getColocatedWith();
  }
  if (colocatedWith == NULLPTR) {
    newCptr = SendClientPRMetadata(regionFullPath, cptr);
    // now we will get new instance so assign it again
    if (newCptr != NULLPTR) {
      cptr->setPreviousone(NULLPTR);
      newCptr->setPreviousone(cptr);
      WriteGuard guard(m_regionMetadataLock);
      m_regionMetaDataMap[path] = newCptr;
      LOGINFO("Updated client meta data");
    }
  } else {
    {
      // ACE_Guard< ACE_Recursive_Thread_Mutex > guard( m_regionMetadataLock );
      // m_regionMetaDataMap[colocatedWith->asChar()] = cptr;
    }
    newCptr = SendClientPRMetadata(colocatedWith->asChar(), cptr);

    if (newCptr != NULLPTR) {
      cptr->setPreviousone(NULLPTR);
      newCptr->setPreviousone(cptr);
      // now we will get new instance so assign it again
      WriteGuard guard(m_regionMetadataLock);
      m_regionMetaDataMap[colocatedWith->asChar()] = newCptr;
      m_regionMetaDataMap[path] = newCptr;
      LOGINFO("Updated client meta data");
    }
  }
}

ClientMetadataPtr ClientMetadataService::SendClientPRMetadata(
    const char* regionPath, ClientMetadataPtr cptr) {
  ThinClientPoolDM* tcrdm = dynamic_cast<ThinClientPoolDM*>(m_pool.ptr());
  if (tcrdm == NULL) {
    throw IllegalArgumentException(
        "ClientMetaData: pool cast to ThinClientPoolDM failed");
  }
  TcrMessageGetClientPrMetadata request(regionPath);
  TcrMessageReply reply(true, NULL);
  // send this message to server and get metadata from server.
  LOGFINE("Now sending GET_CLIENT_PR_METADATA for getting from server: %s",
          regionPath);
  RegionPtr region = NULLPTR;
  GfErrType err = tcrdm->sendSyncRequest(request, reply);
  if (err == GF_NOERR &&
      reply.getMessageType() == TcrMessage::RESPONSE_CLIENT_PR_METADATA) {
    tcrdm->getConnectionManager().getCacheImpl()->getRegion(regionPath, region);
    if (region != NULLPTR) {
      LocalRegion* lregion = dynamic_cast<LocalRegion*>(region.ptr());
      lregion->getRegionStats()->incMetaDataRefreshCount();
    }
    std::vector<BucketServerLocationsType>* metadata = reply.getMetadata();
    if (metadata == NULL) return NULLPTR;
    if (metadata->empty()) {
      delete metadata;
      return NULLPTR;
    }
    ClientMetadata* newCptr = new ClientMetadata(*(cptr.ptr()));
    for (std::vector<BucketServerLocationsType>::iterator iter =
             metadata->begin();
         iter != metadata->end(); ++iter) {
      if (!(*iter).empty()) {
        newCptr->updateBucketServerLocations((*iter).at(0)->getBucketId(),
                                             (*iter));
      }
    }
    delete metadata;
    ClientMetadataPtr newCMDPtr(newCptr);
    return newCMDPtr;
  }
  return NULLPTR;
}

void ClientMetadataService::getBucketServerLocation(
    const RegionPtr& region, const CacheableKeyPtr& key,
    const CacheablePtr& value, const UserDataPtr& aCallbackArgument,
    bool isPrimary, BucketServerLocationPtr& serverLocation, int8_t& version) {
  // ACE_Guard< ACE_Recursive_Thread_Mutex > guard( m_regionMetadataLock );
  if (region != NULLPTR) {
    ReadGuard guard(m_regionMetadataLock);
    LOGDEBUG(
        "ClientMetadataService::getBucketServerLocation m_regionMetaDataMap "
        "size is %d",
        m_regionMetaDataMap.size());
    std::string path(region->getFullPath());
    ClientMetadataPtr cptr = NULLPTR;
    RegionMetadataMapType::iterator itr = m_regionMetaDataMap.find(path);
    if (itr != m_regionMetaDataMap.end()) {
      cptr = itr->second;
    }
    // ClientMetadataPtr cptr = m_regionMetaDataMap[path];
    if (cptr == NULLPTR) {
      // serverLocation = BucketServerLocation();
      return;
    }
    CacheableKeyPtr resolvekey;
    const PartitionResolverPtr& resolver =
        region->getAttributes()->getPartitionResolver();

    EntryEvent event(region, key, value, NULLPTR, aCallbackArgument, false);
    int bucketId = 0;
    if (resolver == NULLPTR) {
      resolvekey = key;
    } else {
      resolvekey = resolver->getRoutingObject(event);
      if (resolvekey == NULLPTR) {
        throw IllegalStateException(
            "The RoutingObject returned by PartitionResolver is null.");
      }
    }
    FixedPartitionResolverPtr fpResolver(
        dynamic_cast<FixedPartitionResolver*>(resolver.ptr()));
    if (fpResolver != NULLPTR) {
      const char* partition = fpResolver->getPartitionName(event);
      if (partition == NULL) {
        throw IllegalStateException(
            "partition name returned by Partition resolver is null.");
      } else {
        bucketId = cptr->assignFixedBucketId(partition, resolvekey);
        if (bucketId == -1) {
          return;
        }
      }
    } else {
      if (cptr->getTotalNumBuckets() > 0) {
        bucketId = std::abs(static_cast<int>(resolvekey->hashcode()) %
                            cptr->getTotalNumBuckets());
      }
    }
    cptr->getServerLocation(bucketId, isPrimary, serverLocation, version);
  }
}

void ClientMetadataService::removeBucketServerLocation(
    BucketServerLocation serverLocation) {
  ReadGuard guard(m_regionMetadataLock);
  for (RegionMetadataMapType::iterator regionMetadataIter =
           m_regionMetaDataMap.begin();
       regionMetadataIter != m_regionMetaDataMap.end(); regionMetadataIter++) {
    ClientMetadataPtr cptr = (*regionMetadataIter).second;
    if (cptr != NULLPTR) {
      // Yogesh has commented out this as it was causing a SIGV
      // cptr->removeBucketServerLocation(serverLocation);
    }
  }
}

ClientMetadataPtr ClientMetadataService::getClientMetadata(
    const char* regionFullPath) {
  ReadGuard guard(m_regionMetadataLock);
  RegionMetadataMapType::iterator regionMetadataIter =
      m_regionMetaDataMap.find(regionFullPath);
  if (regionMetadataIter != m_regionMetaDataMap.end()) {
    return (*regionMetadataIter).second;
  }
  return NULLPTR;
}

/*const  PartitionResolverPtr& ClientMetadataService::getResolver(const
  RegionPtr& region, const CacheableKeyPtr& key,
   const UserDataPtr& aCallbackArgument){
     //const char * regionFullPath = region->getFullPath();
     //if (regionFullPath != NULL) {
       //const RegionAttributesPtr& rAttrsPtr = region->getAttributes();
       return region->getAttributes()->getPartitionResolver();
     //}
  }*/

/*BucketServerLocation
ClientMetadataService::getServerLocation(ClientMetadataPtr cptr, int bucketId,
bool tryPrimary)
{
LOGFINE("Inside getServerLocation");
if (cptr == NULLPTR) {
LOGDEBUG("MetaData does not exist");
return BucketServerLocation();
}
LOGFINE("Ending getServerLocation");
return cptr->getServerLocation(bucketId, tryPrimary);
}*/

void ClientMetadataService::populateDummyServers(const char* regionName,
                                                 ClientMetadataPtr cptr) {
  WriteGuard guard(m_regionMetadataLock);
  m_regionMetaDataMap[regionName] = cptr;
}

void ClientMetadataService::enqueueForMetadataRefresh(
    const char* regionFullPath, int8 serverGroupFlag) {
  ThinClientPoolDM* tcrdm = dynamic_cast<ThinClientPoolDM*>(m_pool.ptr());
  if (tcrdm == NULL) {
    throw IllegalArgumentException(
        "ClientMetaData: pool cast to ThinClientPoolDM failed");
  }
  RegionPtr region;
  tcrdm->getConnectionManager().getCacheImpl()->getRegion(regionFullPath,
                                                          region);
  LocalRegion* lregion = dynamic_cast<LocalRegion*>(region.ptr());
  lregion->getRegionStats()
      ->incNonSingleHopCount();  // we are here means nonSinglehop

  std::string serverGroup = tcrdm->getServerGroup();
  if (serverGroup.length() != 0) {
    CacheImpl::setServerGroupFlag(serverGroupFlag);
    if (serverGroupFlag == 2) {
      LOGFINER(
          "Network hop but, from within same server-group, so no metadata "
          "fetch from the server");
      return;
    }
  }

  if (region != NULLPTR) {
    ThinClientRegion* tcrRegion = dynamic_cast<ThinClientRegion*>(region.ptr());
    {
      TryWriteGuard guardRegionMetaDataRefresh(
          tcrRegion->getMataDataMutex(), tcrRegion->getMetaDataRefreshed());
      if (tcrRegion->getMetaDataRefreshed()) {
        return;
      }
      LOGFINE("Network hop so fetching single hop metadata from the server");
      CacheImpl::setNetworkHopFlag(true);
      tcrRegion->setMetaDataRefreshed(true);
      std::string* tempRegionPath = new std::string(regionFullPath);
      m_regionQueue->put(tempRegionPath);
      m_regionQueueSema.release();
    }
  }
}

HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>*
ClientMetadataService::getServerToFilterMap(const VectorOfCacheableKey* keys,
                                            const RegionPtr& region,
                                            bool isPrimary) {
  // const char* regionFullPath = region->getFullPath();
  ClientMetadataPtr cptr = NULLPTR;
  {
    ReadGuard guard(m_regionMetadataLock);
    RegionMetadataMapType::iterator cptrIter =
        m_regionMetaDataMap.find(region->getFullPath());

    if (cptrIter != m_regionMetaDataMap.end()) {
      cptr = cptrIter->second;
    }

    if (cptr == NULLPTR || keys == NULL) {
      // enqueueForMetadataRefresh(region->getFullPath());
      return NULL;
      //		//serverLocation = BucketServerLocation();
      //		return;
    }
  }
  // int totalNumberOfBuckets = cptr->getTotalNumBuckets();
  HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>* result =
      new HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>();
  VectorOfCacheableKeyPtr keysWhichLeft(new VectorOfCacheableKey());

  std::map<int, BucketServerLocationPtr> buckets;

  for (VectorOfCacheableKey::Iterator iter = keys->begin(); iter != keys->end();
       iter++) {
    CacheableKeyPtr key = *iter;
    LOGDEBUG("cmds = %s", key->toString()->toString());
    PartitionResolverPtr resolver =
        region->getAttributes()->getPartitionResolver();
    CacheableKeyPtr resolveKey;

    if (resolver == NULLPTR) {
      // client has not registered PartitionResolver
      // Assuming even PR at server side is not using PartitionResolver
      resolveKey = key;
    } else {
      EntryEvent event(region, key, NULLPTR, NULLPTR, NULLPTR, false);
      resolveKey = resolver->getRoutingObject(event);
    }

    int bucketId = std::abs(static_cast<int>(resolveKey->hashcode()) %
                            cptr->getTotalNumBuckets());
    VectorOfCacheableKeyPtr keyList = NULLPTR;
    std::map<int, BucketServerLocationPtr>::iterator bucketsIter =
        buckets.find(bucketId);

    if (bucketsIter == buckets.end()) {
      int8 version = -1;
      // BucketServerLocationPtr serverLocation(new BucketServerLocation());
      BucketServerLocationPtr serverLocation = NULLPTR;
      cptr->getServerLocation(bucketId, isPrimary, serverLocation, version);
      if (serverLocation == NULLPTR) {  //:if server not returns all buckets,
                                        // need to confiem with PR team about
        // this why??
        keysWhichLeft->push_back(key);
        continue;
      } else if (!serverLocation->isValid()) {
        keysWhichLeft->push_back(key);
        continue;
      }
      // if(serverLocation == NULLPTR)
      // continue;// need to fix
      buckets[bucketId] = serverLocation;
      HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>::Iterator
          itrRes = result->find(serverLocation);
      // keyList = (*result)[serverLocation];

      if (itrRes == result->end()) {
        keyList = new VectorOfCacheableKey();
        result->insert(serverLocation, keyList);
      } else {
        keyList = itrRes.second();
      }
      LOGDEBUG("new keylist buckets =%d res = %d", buckets.size(),
               result->size());
    } else {
      keyList = (*result)[bucketsIter->second];
    }

    keyList->push_back(key);
  }

  if (keysWhichLeft->size() > 0 &&
      result->size() > 0) {  // add left keys in result
    int keyLefts = keysWhichLeft->size();
    int totalServers = result->size();
    int perServer = keyLefts / totalServers + 1;

    int keyIdx = 0;
    for (HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>::Iterator
             locationIter = result->begin();
         locationIter != result->end(); locationIter++) {
      VectorOfCacheableKeyPtr keys = locationIter.second();
      for (int i = 0; i < perServer; i++) {
        if (keyIdx < keyLefts) {
          keys->push_back(keysWhichLeft->at(keyIdx++));
        } else {
          break;
        }
      }
      if (keyIdx >= keyLefts) break;  // done
    }
  } else if (result->size() == 0) {  // not be able to map any key
    return NULL;  // it will force all keys to send to one server
  }

  return result;
}

void ClientMetadataService::markPrimaryBucketForTimeout(
    const RegionPtr& region, const CacheableKeyPtr& key,
    const CacheablePtr& value, const UserDataPtr& aCallbackArgument,
    bool isPrimary, BucketServerLocationPtr& serverLocation, int8_t& version) {
  if (m_bucketWaitTimeout == 0) return;

  WriteGuard guard(m_PRbucketStatusLock);

  getBucketServerLocation(region, key, value, aCallbackArgument,
                          false /*look for secondary host*/, serverLocation,
                          version);

  if (serverLocation != NULLPTR && serverLocation->isValid()) {
    LOGDEBUG("Server host and port are %s:%d",
             serverLocation->getServerName().c_str(),
             serverLocation->getPort());
    int32_t bId = serverLocation->getBucketId();

    std::map<std::string, PRbuckets*>::iterator bs =
        m_bucketStatus.find(region->getFullPath());

    if (bs != m_bucketStatus.end()) {
      bs->second->setBucketTimeout(bId);
      LOGDEBUG("marking bucket %d as timeout ", bId);
    }
  }
}

HashMapT<CacheableInt32Ptr, CacheableHashSetPtr>*
ClientMetadataService::groupByBucketOnClientSide(const RegionPtr& region,
                                                 CacheableVectorPtr* keySet,
                                                 ClientMetadataPtr& metadata) {
  HashMapT<CacheableInt32Ptr, CacheableHashSetPtr>* bucketToKeysMap =
      new HashMapT<CacheableInt32Ptr, CacheableHashSetPtr>();
  for (CacheableVector::Iterator itr = (*keySet)->begin();
       itr != (*keySet)->end(); ++itr) {
    CacheableKeyPtr key = dynCast<CacheableKeyPtr>(*itr);
    PartitionResolverPtr resolver =
        region->getAttributes()->getPartitionResolver();
    CacheableKeyPtr resolvekey;
    EntryEvent event(region, key, NULLPTR, NULLPTR, NULLPTR, false);
    int bucketId = -1;
    if (resolver == NULLPTR) {
      resolvekey = key;
    } else {
      resolvekey = resolver->getRoutingObject(event);
      if (resolvekey == NULLPTR) {
        throw IllegalStateException(
            "The RoutingObject returned by PartitionResolver is null.");
      }
    }
    FixedPartitionResolverPtr fpResolver(
        dynamic_cast<FixedPartitionResolver*>(resolver.ptr()));
    if (fpResolver != NULLPTR) {
      const char* partition = fpResolver->getPartitionName(event);
      if (partition == NULL) {
        throw IllegalStateException(
            "partition name returned by Partition resolver is null.");
      } else {
        bucketId = metadata->assignFixedBucketId(partition, resolvekey);
        if (bucketId == -1) {
          this->enqueueForMetadataRefresh(region->getFullPath(), 0);
        }
      }
    } else {
      if (metadata->getTotalNumBuckets() > 0) {
        bucketId = std::abs(static_cast<int>(resolvekey->hashcode()) %
                            metadata->getTotalNumBuckets());
      }
    }
    HashMapT<CacheableInt32Ptr, CacheableHashSetPtr>::Iterator iter =
        bucketToKeysMap->find(CacheableInt32::create(bucketId));
    CacheableHashSetPtr bucketKeys;
    if (iter == bucketToKeysMap->end()) {
      bucketKeys = CacheableHashSet::create();
      bucketToKeysMap->insert(CacheableInt32::create(bucketId), bucketKeys);
    } else {
      bucketKeys = iter.second();
    }
    bucketKeys->insert(key);
  }
  return bucketToKeysMap;
}

HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>*
ClientMetadataService::getServerToFilterMapFESHOP(
    CacheableVectorPtr* routingKeys, const RegionPtr& region, bool isPrimary) {
  ClientMetadataPtr cptr = getClientMetadata(region->getFullPath());

  if (cptr == NULLPTR /*|| cptr->adviseRandomServerLocation() == NULLPTR*/) {
    enqueueForMetadataRefresh(region->getFullPath(), 0);
    return NULL;
  }

  if (routingKeys == NULL) {
    return NULL;
  }

  HashMapT<CacheableInt32Ptr, CacheableHashSetPtr>* bucketToKeysMap =
      groupByBucketOnClientSide(region, routingKeys, cptr);
  CacheableHashSetPtr bucketSet = CacheableHashSet::create();
  for (HashMapT<CacheableInt32Ptr, CacheableHashSetPtr>::Iterator iter =
           bucketToKeysMap->begin();
       iter != bucketToKeysMap->end(); ++iter) {
    bucketSet->insert(iter.first());
  }
  LOGDEBUG(
      "ClientMetadataService::getServerToFilterMapFESHOP: bucketSet size = %d ",
      bucketSet->size());

  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* serverToBuckets =
      groupByServerToBuckets(cptr, bucketSet, isPrimary);

  if (serverToBuckets == NULL) {
    delete bucketToKeysMap;
    return NULL;
  }

  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* serverToKeysMap =
      new HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>();

  for (HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator itrRes =
           serverToBuckets->begin();
       itrRes != serverToBuckets->end(); ++itrRes) {
    BucketServerLocationPtr serverLocation = itrRes.first();
    CacheableHashSetPtr buckets = itrRes.second();
    for (CacheableHashSet::Iterator bucket = buckets->begin();
         bucket != buckets->end(); ++bucket) {
      HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator iter =
          serverToKeysMap->find(serverLocation);
      CacheableHashSetPtr keys;
      if (iter == serverToKeysMap->end()) {
        keys = CacheableHashSet::create();
      } else {
        keys = iter.second();
      }
      HashMapT<CacheableInt32Ptr, CacheableHashSetPtr>::Iterator
          bucketToKeysiter = bucketToKeysMap->find(*bucket);
      if (bucketToKeysiter != bucketToKeysMap->end()) {
        CacheableHashSetPtr bkeys = bucketToKeysiter.second();
        for (CacheableHashSet::Iterator itr = bkeys->begin();
             itr != bkeys->end(); ++itr) {
          keys->insert(*itr);
        }
      }
      serverToKeysMap->insert(serverLocation, keys);
    }
  }
  delete bucketToKeysMap;
  delete serverToBuckets;
  return serverToKeysMap;
}

BucketServerLocationPtr ClientMetadataService::findNextServer(
    HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* serverToBucketsMap,
    CacheableHashSetPtr& currentBucketSet) {
  BucketServerLocationPtr serverLocation;
  int max = -1;
  std::vector<BucketServerLocationPtr> nodesOfEqualSize;
  for (HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator itr =
           serverToBucketsMap->begin();
       itr != serverToBucketsMap->end(); ++itr) {
    CacheableHashSetPtr buckets = CacheableHashSet::create();
    CacheableHashSetPtr sBuckets = itr.second();

    for (CacheableHashSet::Iterator sItr = sBuckets->begin();
         sItr != sBuckets->end(); ++sItr) {
      buckets->insert(*sItr);
    }

    LOGDEBUG(
        "ClientMetadataService::findNextServer currentBucketSet->size() = %d  "
        "bucketSet->size() = %d ",
        currentBucketSet->size(), buckets->size());

    for (CacheableHashSet::Iterator currentBucketSetIter =
             currentBucketSet->begin();
         currentBucketSetIter != currentBucketSet->end();
         ++currentBucketSetIter) {
      buckets->erase(*currentBucketSetIter);
      LOGDEBUG("ClientMetadataService::findNextServer bucketSet->size() = %d ",
               buckets->size());
    }

    int size = buckets->size();
    if (max < size) {
      max = size;
      serverLocation = itr.first();
      nodesOfEqualSize.clear();
      nodesOfEqualSize.push_back(serverLocation);
    } else if (max == size) {
      nodesOfEqualSize.push_back(serverLocation);
    }
  }

  size_t nodeSize = nodesOfEqualSize.size();
  if (nodeSize > 0) {
    RandGen randgen;
    int random = randgen(nodeSize);
    return nodesOfEqualSize.at(random);
  }
  return NULLPTR;
}

bool ClientMetadataService::AreBucketSetsEqual(
    CacheableHashSetPtr& currentBucketSet, CacheableHashSetPtr& bucketSet) {
  int32_t currentBucketSetSize = currentBucketSet->size();
  int32_t bucketSetSetSize = bucketSet->size();

  LOGDEBUG(
      "ClientMetadataService::AreBucketSetsEqual currentBucketSetSize = %d "
      "bucketSetSetSize = %d ",
      currentBucketSetSize, bucketSetSetSize);

  if (currentBucketSetSize != bucketSetSetSize) {
    return false;
  }

  bool found = false;
  for (CacheableHashSet::Iterator currentBucketSetIter =
           currentBucketSet->begin();
       currentBucketSetIter != currentBucketSet->end();
       ++currentBucketSetIter) {
    found = false;
    for (CacheableHashSet::Iterator bucketSetIter = bucketSet->begin();
         bucketSetIter != bucketSet->end(); ++bucketSetIter) {
      if (*currentBucketSetIter == *bucketSetIter) {
        found = true;
        break;
      }
    }
    if (!found) return false;
  }
  return true;
}

HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>*
ClientMetadataService::pruneNodes(ClientMetadataPtr& metadata,
                                  CacheableHashSetPtr& buckets) {
  CacheableHashSetPtr bucketSetWithoutServer = CacheableHashSet::create();
  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* serverToBucketsMap =
      new HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>();
  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>*
      prunedServerToBucketsMap =
          new HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>();

  for (CacheableHashSet::Iterator bucketId = buckets->begin();
       bucketId != buckets->end(); ++bucketId) {
    CacheableInt32Ptr bID = *bucketId;
    std::vector<BucketServerLocationPtr> locations =
        metadata->adviseServerLocations(bID->value());
    if (locations.size() == 0) {
      LOGDEBUG(
          "ClientMetadataService::pruneNodes Since no server location "
          "available for bucketId = %d  putting it into "
          "bucketSetWithoutServer ",
          bID->value());
      bucketSetWithoutServer->insert(bID);
      continue;
    }

    for (std::vector<BucketServerLocationPtr>::iterator location =
             locations.begin();
         location != locations.end(); ++location) {
      HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator itrRes =
          serverToBucketsMap->find(*location);
      CacheableHashSetPtr bucketSet;
      if (itrRes == serverToBucketsMap->end()) {
        bucketSet = CacheableHashSet::create();
      } else {
        bucketSet = itrRes.second();
      }
      bucketSet->insert(bID);
      serverToBucketsMap->insert(*location, bucketSet);
    }
  }

  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator itrRes =
      serverToBucketsMap->begin();
  CacheableHashSetPtr currentBucketSet = CacheableHashSet::create();
  BucketServerLocationPtr randomFirstServer;
  if (serverToBucketsMap->empty()) {
    LOGDEBUG(
        "ClientMetadataService::pruneNodes serverToBucketsMap is empty so "
        "returning NULL");
    delete prunedServerToBucketsMap;
    delete serverToBucketsMap;
    return NULL;
  } else {
    size_t size = serverToBucketsMap->size();
    LOGDEBUG(
        "ClientMetadataService::pruneNodes Total size of serverToBucketsMap = "
        "%d ",
        size);
    for (size_t idx = 0; idx < (rand() % size); idx++) {
      itrRes++;
    }
    randomFirstServer = itrRes.first();
  }
  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator itrRes1 =
      serverToBucketsMap->find(randomFirstServer);
  CacheableHashSetPtr bucketSet = itrRes1.second();

  for (CacheableHashSet::Iterator bt = bucketSet->begin();
       bt != bucketSet->end(); ++bt) {
    currentBucketSet->insert(*bt);
  }
  prunedServerToBucketsMap->insert(randomFirstServer, bucketSet);
  serverToBucketsMap->erase(randomFirstServer);

  while (!AreBucketSetsEqual(currentBucketSet, buckets)) {
    BucketServerLocationPtr server =
        findNextServer(serverToBucketsMap, currentBucketSet);
    if (server == NULLPTR) {
      LOGDEBUG(
          "ClientMetadataService::pruneNodes findNextServer returned no "
          "server");
      break;
    }

    HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator itrRes2 =
        serverToBucketsMap->find(server);
    CacheableHashSetPtr bucketSet2 = itrRes2.second();

    LOGDEBUG(
        "ClientMetadataService::pruneNodes currentBucketSet->size() = %d  "
        "bucketSet2->size() = %d ",
        currentBucketSet->size(), bucketSet2->size());

    for (CacheableHashSet::Iterator currentBucketSetIter =
             currentBucketSet->begin();
         currentBucketSetIter != currentBucketSet->end();
         ++currentBucketSetIter) {
      bucketSet2->erase(*currentBucketSetIter);
      LOGDEBUG("ClientMetadataService::pruneNodes bucketSet2->size() = %d ",
               bucketSet2->size());
    }

    if (bucketSet2->empty()) {
      LOGDEBUG(
          "ClientMetadataService::pruneNodes bucketSet2 is empty() so removing "
          "server from serverToBucketsMap");
      serverToBucketsMap->erase(server);
      continue;
    }

    for (CacheableHashSet::Iterator itr = bucketSet2->begin();
         itr != bucketSet2->end(); ++itr) {
      currentBucketSet->insert(*itr);
    }

    prunedServerToBucketsMap->insert(server, bucketSet2);
    serverToBucketsMap->erase(server);
  }

  HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator itrRes2 =
      prunedServerToBucketsMap->begin();
  for (CacheableHashSet::Iterator itr = bucketSetWithoutServer->begin();
       itr != bucketSetWithoutServer->end(); ++itr) {
    CacheableInt32Ptr buckstId = *itr;
    itrRes2.second()->insert(buckstId);
  }

  delete serverToBucketsMap;
  return prunedServerToBucketsMap;
}

HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>*
ClientMetadataService::groupByServerToAllBuckets(const RegionPtr& region,
                                                 bool optimizeForWrite) {
  ClientMetadataPtr cptr = getClientMetadata(region->getFullPath());
  if (cptr == NULLPTR) {
    enqueueForMetadataRefresh(region->getFullPath(), false);
    return NULL;
  }
  int totalBuckets = cptr->getTotalNumBuckets();
  CacheableHashSetPtr bucketSet = CacheableHashSet::create();
  for (int i = 0; i < totalBuckets; i++) {
    bucketSet->insert(CacheableInt32::create(i));
  }
  return groupByServerToBuckets(cptr, bucketSet, optimizeForWrite);
}

HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>*
ClientMetadataService::groupByServerToBuckets(ClientMetadataPtr& metadata,
                                              CacheableHashSetPtr& bucketSet,
                                              bool optimizeForWrite) {
  if (optimizeForWrite) {
    HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* serverToBucketsMap =
        new HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>();
    CacheableHashSetPtr bucketsWithoutServer = CacheableHashSet::create();
    for (CacheableHashSet::Iterator itr = bucketSet->begin();
         itr != bucketSet->end(); ++itr) {
      CacheableInt32Ptr bucketId = *itr;
      BucketServerLocationPtr serverLocation =
          metadata->advisePrimaryServerLocation(bucketId->value());
      if (serverLocation == NULLPTR) {
        bucketsWithoutServer->insert(bucketId);
        continue;
      } else if (!serverLocation->isValid()) {
        bucketsWithoutServer->insert(bucketId);
        continue;
      }
      HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator itrRes =
          serverToBucketsMap->find(serverLocation);
      CacheableHashSetPtr buckets;
      if (itrRes == serverToBucketsMap->end()) {
        buckets = CacheableHashSet::create();
        serverToBucketsMap->insert(serverLocation, buckets);
      } else {
        buckets = itrRes.second();
      }
      buckets->insert(bucketId);
    }

    if (!serverToBucketsMap->empty()) {
      HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator itrRes =
          serverToBucketsMap->begin();
      for (CacheableHashSet::Iterator itr = bucketsWithoutServer->begin();
           itr != bucketsWithoutServer->end(); ++itr) {
        itrRes.second()->insert(*itr);
        LOGDEBUG(
            "ClientMetadataService::groupByServerToBuckets inserting "
            "bucketsWithoutServer");
      }
    }
    return serverToBucketsMap;
  } else {
    return pruneNodes(metadata, bucketSet);
  }
}

void ClientMetadataService::markPrimaryBucketForTimeoutButLookSecondaryBucket(
    const RegionPtr& region, const CacheableKeyPtr& key,
    const CacheablePtr& value, const UserDataPtr& aCallbackArgument,
    bool isPrimary, BucketServerLocationPtr& serverLocation, int8_t& version) {
  if (m_bucketWaitTimeout == 0) return;

  WriteGuard guard(m_PRbucketStatusLock);

  std::map<std::string, PRbuckets*>::iterator bs =
      m_bucketStatus.find(region->getFullPath());

  PRbuckets* prBuckets = NULL;
  if (bs != m_bucketStatus.end()) {
    prBuckets = bs->second;
  }

  if (prBuckets == NULL) return;

  getBucketServerLocation(region, key, value, aCallbackArgument, true,
                          serverLocation, version);

  ClientMetadataPtr cptr = NULLPTR;
  {
    ReadGuard guard(m_regionMetadataLock);
    RegionMetadataMapType::iterator cptrIter =
        m_regionMetaDataMap.find(region->getFullPath());

    if (cptrIter != m_regionMetaDataMap.end()) {
      cptr = cptrIter->second;
    }

    if (cptr == NULLPTR) {
      return;
    }
  }

  LOGFINE("Setting in markPrimaryBucketForTimeoutButLookSecondaryBucket");

  int32_t totalBuckets = cptr->getTotalNumBuckets();

  for (int32_t i = 0; i < totalBuckets; i++) {
    int8_t version;
    BucketServerLocationPtr bsl;
    cptr->getServerLocation(i, false, bsl, version);

    if (bsl == serverLocation) {
      prBuckets->setBucketTimeout(i);
      LOGFINE(
          "markPrimaryBucketForTimeoutButLookSecondaryBucket::setting bucket "
          "timeout...");
    }
  }
}

bool ClientMetadataService::isBucketMarkedForTimeout(const char* regionFullPath,
                                                     int32_t bucketid) {
  if (m_bucketWaitTimeout == 0) return false;

  ReadGuard guard(m_PRbucketStatusLock);

  std::map<std::string, PRbuckets*>::iterator bs =
      m_bucketStatus.find(regionFullPath);

  if (bs != m_bucketStatus.end()) {
    bool m = bs->second->isBucketTimedOut(bucketid, m_bucketWaitTimeout);
    if (m == true) {
      ThinClientPoolDM* tcrdm = dynamic_cast<ThinClientPoolDM*>(m_pool.ptr());
      CacheImpl* cache = tcrdm->getConnectionManager().getCacheImpl();
      cache->setBlackListBucketTimeouts();
    }
    LOGFINE("isBucketMarkedForTimeout:: for bucket %d returning = %d", bucketid,
            m);

    return m;
  }

  return false;
}
}  // namespace gemfire
