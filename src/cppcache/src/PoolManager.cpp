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
