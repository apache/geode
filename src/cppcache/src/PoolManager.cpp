/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/PoolManager.hpp>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>

using namespace gemfire;

// TODO: make this a member of TcrConnectionManager.
HashMapOfPools* connectionPools = NULL; /*new HashMapOfPools( )*/
ACE_Recursive_Thread_Mutex connectionPoolsLock;

void removePool(const char* name) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(connectionPoolsLock);
  connectionPools->erase(CacheableString::create(name));
}

PoolFactoryPtr PoolManager::createFactory() {
  if (connectionPools == NULL) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(connectionPoolsLock);
    if (connectionPools == NULL) {
      connectionPools = new HashMapOfPools();
    }
  }
  return PoolFactoryPtr(new PoolFactory());
}

void PoolManager::close(bool keepAlive) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(connectionPoolsLock);

  if (connectionPools == NULL) {
    return;
  }

  std::vector<PoolPtr> poolsList;

  for (HashMapOfPools::Iterator iter = connectionPools->begin();
       iter != connectionPools->end(); ++iter) {
    PoolPtr currPool(iter.second());
    poolsList.push_back(currPool);
  }

  for (std::vector<PoolPtr>::iterator iter = poolsList.begin();
       iter != poolsList.end(); ++iter) {
    (*iter)->destroy(keepAlive);
  }

  GF_SAFE_DELETE(connectionPools);
}

PoolPtr PoolManager::find(const char* name) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(connectionPoolsLock);

  if (connectionPools == NULL) {
    connectionPools = new HashMapOfPools();
  }

  if (name) {
    HashMapOfPools::Iterator iter =
        connectionPools->find(CacheableString::create(name));

    PoolPtr poolPtr = NULLPTR;

    if (iter != connectionPools->end()) {
      poolPtr = iter.second();
      GF_DEV_ASSERT(poolPtr != NULLPTR);
    }

    return poolPtr;
  } else {
    return NULLPTR;
  }
}

PoolPtr PoolManager::find(RegionPtr region) {
  return find(region->getAttributes()->getPoolName());
}

const HashMapOfPools& PoolManager::getAll() {
  if (connectionPools == NULL) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(connectionPoolsLock);
    if (connectionPools == NULL) {
      connectionPools = new HashMapOfPools();
    }
  }
  return *connectionPools;
}
