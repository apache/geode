/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ClientMetadata.hpp"
#include "Utils.hpp"
#include "ThinClientPoolDM.hpp"
#include <cstdlib>
#include <climits>
#include <ace/OS.h>

namespace gemfire {

ClientMetadata::ClientMetadata(
    int totalNumBuckets, CacheableStringPtr colocatedWith,
    ThinClientPoolDM* tcrdm,
    std::vector<FixedPartitionAttributesImplPtr>* fpaSet)
    : m_partitionNames(NULLPTR),
      m_previousOne(NULLPTR),
      m_totalNumBuckets(totalNumBuckets),
      m_colocatedWith(colocatedWith),
      m_tcrdm(tcrdm) {
  LOGFINE("Creating metadata with %d buckets", totalNumBuckets);
  for (int item = 0; item < totalNumBuckets; item++) {
    BucketServerLocationsType empty;
    m_bucketServerLocationsList.push_back(empty);
  }
  if (m_tcrdm == NULL) {
    throw IllegalArgumentException("ClientMetaData: ThinClientPoolDM is NULL.");
  }
  if (fpaSet != NULL) {
    LOGDEBUG(
        "ClientMetadata Creating metadata with %d buckets & fpaset size is %d ",
        totalNumBuckets, fpaSet->size());
    if (!fpaSet->empty()) {
      int totalFPABuckets = 0;
      for (int i = 0; i < static_cast<int>(fpaSet->size()); i++) {
        std::vector<int> attList;
        totalFPABuckets += fpaSet->at(i)->getNumBuckets();
        attList.push_back(fpaSet->at(i)->getNumBuckets());
        attList.push_back(fpaSet->at(i)->getStartingBucketID());
        m_fpaMap[fpaSet->at(i)->getPartitionName()] = attList;
      }
      setPartitionNames();
    }
    delete fpaSet;
  }
}

ClientMetadata::ClientMetadata(ClientMetadata& other) {
  m_partitionNames = NULLPTR;
  m_previousOne = NULLPTR;
  m_totalNumBuckets = other.m_totalNumBuckets;
  for (int item = 0; item < m_totalNumBuckets; item++) {
    BucketServerLocationsType empty;
    m_bucketServerLocationsList.push_back(empty);
  }
  m_colocatedWith = other.m_colocatedWith;
  m_tcrdm = other.m_tcrdm;
  for (FixedMapType::iterator iter = other.m_fpaMap.begin();
       iter != other.m_fpaMap.end(); ++iter) {
    m_fpaMap[iter->first] = iter->second;
  }
  if (!m_fpaMap.empty()) setPartitionNames();
}

ClientMetadata::ClientMetadata()
    : m_partitionNames(NULLPTR),
      m_previousOne(NULLPTR),
      m_totalNumBuckets(0),
      m_colocatedWith(NULLPTR),
      m_tcrdm(NULL) {}

ClientMetadata::~ClientMetadata() {}

void ClientMetadata::setPartitionNames() {
  m_partitionNames = CacheableHashSet::create();
  for (FixedMapType::iterator it = m_fpaMap.begin(); it != m_fpaMap.end();
       it++) {
    m_partitionNames->insert(CacheableString::create(((*it).first).c_str()));
  }
}

int ClientMetadata::getTotalNumBuckets() {
  // ReadGuard guard (m_readWriteLock);
  return m_totalNumBuckets;
}

/*PartitionResolverPtr ClientMetadata::getPartitionResolver()
{
  ReadGuard guard (m_readWriteLock);
  LOGFINE("Inside getPartitionResolver");
  return m_partitionResolver;
}*/

CacheableStringPtr ClientMetadata::getColocatedWith() {
  // ReadGuard guard (m_readWriteLock);
  return m_colocatedWith;
}

void ClientMetadata::getServerLocation(int bucketId, bool tryPrimary,
                                       BucketServerLocationPtr& serverLocation,
                                       int8_t& version) {
  // ReadGuard guard (m_readWriteLock);
  checkBucketId(bucketId);
  // BucketServerLocationsType locations =
  // m_bucketServerLocationsList[bucketId];
  if (m_bucketServerLocationsList[bucketId].empty()) {
    return;
  } else if (tryPrimary) {
    LOGFINER("returning primary & m_bucketServerLocationsList size is %d",
             m_bucketServerLocationsList.size());
    serverLocation = m_bucketServerLocationsList[bucketId].at(0);
    if (serverLocation->isValid()) {
      if (serverLocation->isPrimary()) {
        version = serverLocation->getVersion();
      }
    } else {
      version = serverLocation->getVersion();
    }
  } else {
    serverLocation = m_bucketServerLocationsList[bucketId].at(0);
    if (serverLocation->isValid()) {
      if (serverLocation->isPrimary()) {
        version = serverLocation->getVersion();
      }
    } else {
      version = serverLocation->getVersion();
    }
    RandGen randgen;
    serverLocation = m_bucketServerLocationsList[bucketId].at(randgen(
        static_cast<int>(m_bucketServerLocationsList[bucketId].size())));
  }
  // return m_bucketServerLocationsList[bucketId].at(0);
}

/*
ServerLocation ClientMetadata::getPrimaryServerLocation(int bucketId)
{
  ReadGuard guard (m_readWriteLock);

  checkBucketId(bucketId);

  BucketServerLocationsType locations = m_bucketServerLocationsList[bucketId];

  if (locations.size() > 0 && locations[0].isPrimary()) {
    return locations[0];
  }

  return ServerLocation();
}
*/

void ClientMetadata::updateBucketServerLocations(
    int bucketId, BucketServerLocationsType bucketServerLocations) {
  // WriteGuard guard( m_readWriteLock );
  checkBucketId(bucketId);

  std::string serverGroup = m_tcrdm->getServerGroup();

  // This is for pruning according to server groups, only applicable when client
  // is configured with
  // server-group.
  if (serverGroup.length() != 0) {
    BucketServerLocationsType primaries;
    BucketServerLocationsType secondaries;
    for (BucketServerLocationsType::iterator iter =
             bucketServerLocations.begin();
         iter != bucketServerLocations.end(); ++iter) {
      CacheableStringArrayPtr groups = (*iter)->getServerGroups();
      if ((groups != NULLPTR) && (groups->length() > 0)) {
        bool added = false;
        for (int i = 0; i < groups->length(); i++) {
          CacheableStringPtr cs = groups[i];
          if (cs->length() > 0) {
            std::string str = cs->toString();
            if ((ACE_OS::strcmp(str.c_str(), serverGroup.c_str()) == 0)) {
              added = true;
              if ((*iter)->isPrimary()) {
                primaries.push_back(*iter);
                break;
              } else {
                secondaries.push_back(*iter);
                break;
              }
            }
          } else {
            added = true;
            if ((*iter)->isPrimary()) {
              primaries.push_back(*iter);
            } else {
              secondaries.push_back(*iter);
            }
          }
        }
        if (!added) {
          (*iter)->setServername(NULLPTR);
          if ((*iter)->isPrimary()) {
            primaries.push_back(*iter);
          } else {
            secondaries.push_back(*iter);
          }
        }
      }
    }

    // shuffle the deck

    RandGen randGen;

    if (primaries.size() > 0) {
      std::random_shuffle(primaries.begin(), primaries.end(), randGen);
    }

    if (secondaries.size() > 0) {
      std::random_shuffle(secondaries.begin(), secondaries.end(), randGen);
    }

    m_bucketServerLocationsList[bucketId].clear();
    for (BucketServerLocationsType::iterator iter = primaries.begin();
         iter != primaries.end(); ++iter) {
      LOGFINER("updating primaries with bucketId %d", bucketId);
      m_bucketServerLocationsList[bucketId].push_back(*iter);
    }

    // add secondaries to the end
    for (BucketServerLocationsType::iterator iter = secondaries.begin();
         iter != secondaries.end(); ++iter) {
      LOGFINER("updating secondaries with bucketId %d", bucketId);
      m_bucketServerLocationsList[bucketId].push_back(*iter);
    }
  } else {
    BucketServerLocationsType primaries;
    BucketServerLocationsType secondaries;

    // separate out the primaries from the secondaries

    for (BucketServerLocationsType::iterator iter =
             bucketServerLocations.begin();
         iter != bucketServerLocations.end(); ++iter) {
      if ((*iter)->isPrimary()) {
        primaries.push_back(*iter);
      } else {
        secondaries.push_back(*iter);
      }
    }

    // shuffle the deck

    RandGen randGen;

    if (primaries.size() > 0) {
      std::random_shuffle(primaries.begin(), primaries.end(), randGen);
    }

    if (secondaries.size() > 0) {
      std::random_shuffle(secondaries.begin(), secondaries.end(), randGen);
    }

    m_bucketServerLocationsList[bucketId].clear();

    // add primaries to the front
    for (BucketServerLocationsType::iterator iter = primaries.begin();
         iter != primaries.end(); ++iter) {
      LOGFINER("updating primaries with bucketId %d and Server = %s ", bucketId,
               (*iter)->getEpString().c_str());
      m_bucketServerLocationsList[bucketId].push_back(*iter);
    }

    // add secondaries to the end
    for (BucketServerLocationsType::iterator iter = secondaries.begin();
         iter != secondaries.end(); ++iter) {
      LOGFINER("updating secondaries with bucketId %d", bucketId);
      m_bucketServerLocationsList[bucketId].push_back(*iter);
    }
  }
}

void ClientMetadata::removeBucketServerLocation(
    BucketServerLocation serverLocation) {
  /*WriteGuard guard( m_readWriteLock );
  int i=0;
  bool locnfound = false;
  BucketServerLocationsType locations;
  BucketServerLocationsType::iterator locnsIter;
  BucketServerLocationsListType::iterator iter =
  m_bucketServerLocationsList.begin();
  for(iter = m_bucketServerLocationsList.begin(); iter !=
  m_bucketServerLocationsList.end();
    iter++, i++) {
      locations = *(iter);
      for(locnsIter = locations.begin(); locnsIter != locations.end();
  locnsIter++) {
        LOGFINE("Erasing from inner list its size is %d",locations.size());
        if (locations.erase(std::find(locations.begin(), locations.end(),
  serverLocation)) != locations.end()) {
          locnfound = true;
          LOGFINE("Erased from inner list its size is %d",locations.size());
          break;
        }
      }
      if (locnfound) {
         LOGFINE("Location found");
        break;
      }
    }
  LOGFINE("updating outer list size is & i is %d\t %d",
  m_bucketServerLocationsList.size(),i);
  if (locnfound) {
    m_bucketServerLocationsList[i] = locations;
  }
  LOGFINE("updated outer list size is %d", m_bucketServerLocationsList.size());
  */
}

void ClientMetadata::populateDummyServers(int bucketId,
                                          BucketServerLocationsType locations) {
  // WriteGuard guard( m_readWriteLock );

  checkBucketId(bucketId);

  m_bucketServerLocationsList[bucketId] = locations;
}

int ClientMetadata::assignFixedBucketId(const char* partitionName,
                                        CacheableKeyPtr resolvekey) {
  LOGDEBUG(
      "FPR assignFixedBucketId partititonname = %s , m_fpaMap.size() = %d ",
      partitionName, m_fpaMap.size());
  FixedMapType::iterator iter = m_fpaMap.find(partitionName);
  if (iter != m_fpaMap.end()) {
    std::vector<int> attList = iter->second;
    int hc = static_cast<int>(resolvekey->hashcode());
    int bucketId = std::abs(hc % (attList.at(0)));
    int partitionBucketID = bucketId + attList.at(1);
    return partitionBucketID;
  } else {
    return -1;
  }
}

std::vector<BucketServerLocationPtr> ClientMetadata::adviseServerLocations(
    int bucketId) {
  checkBucketId(bucketId);
  return m_bucketServerLocationsList[bucketId];
}

BucketServerLocationPtr ClientMetadata::advisePrimaryServerLocation(
    int bucketId) {
  std::vector<BucketServerLocationPtr> locations =
      adviseServerLocations(bucketId);
  for (std::vector<BucketServerLocationPtr>::iterator iter = locations.begin();
       iter != locations.end(); ++iter) {
    BucketServerLocationPtr location = *iter;
    if (location->isPrimary()) {
      return location;
    }
  }
  return NULLPTR;
}

BucketServerLocationPtr ClientMetadata::adviseRandomServerLocation() {
  if (m_bucketServerLocationsList.size() > 0) {
    RandGen randGen;
    size_t random = randGen(m_bucketServerLocationsList.size());
    checkBucketId(random);
    std::vector<BucketServerLocationPtr> locations =
        m_bucketServerLocationsList[random];
    if (locations.size() == 0) return NULLPTR;
    return locations.at(0);
  }
  return NULLPTR;
}
}  // namespace gemfire
