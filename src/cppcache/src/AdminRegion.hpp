#pragma once

#ifndef GEODE_ADMINREGION_H_
#define GEODE_ADMINREGION_H_

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

#include <gfcpp/gf_types.hpp>
#include "ThinClientCacheDistributionManager.hpp"
#include "ReadWriteLock.hpp"
#include <gfcpp/Serializable.hpp>
#include <gfcpp/SharedPtr.hpp>
//#include <statistics/HostStatSampler.hpp>

#include "NonCopyable.hpp"
namespace apache {
namespace geode {
namespace statistics {
class HostStatSampler;
}  // namespace statistics
}  // namespace geode
}  // namespace apache

namespace apache {
namespace geode {
namespace client {
class CacheImpl;
/* adongre
 * CID 28724: Other violation (MISSING_COPY)
 * Class "apache::geode::client::AdminRegion" owns resources that are managed in
 * its
 * constructor and destructor but has no user-written copy constructor.
 *
 * CID 28710: Other violation (MISSING_ASSIGN)
 * Class "apache::geode::client::AdminRegion" owns resources that are managed in
 * its
 * constructor and destructor but has no user-written assignment operator.
 *
 * FIX : Make the class noncopyabl3
 */
class AdminRegion : public SharedBase,
                    private NonCopyable,
                    private NonAssignable {
 private:
  ThinClientBaseDM* m_distMngr;
  std::string m_fullPath;
  TcrConnectionManager* m_connectionMgr;
  ACE_RW_Thread_Mutex m_rwLock;
  bool m_destroyPending;

  GfErrType putNoThrow(const CacheableKeyPtr& keyPtr,
                       const CacheablePtr& valuePtr);
  TcrConnectionManager* getConnectionManager();

 public:
  ACE_RW_Thread_Mutex& getRWLock();
  const bool& isDestroyed();
  void close();
  void init();
  void put(const CacheableKeyPtr& keyPtr, const CacheablePtr& valuePtr);
  AdminRegion(CacheImpl* cache, ThinClientBaseDM* distMan = NULL);
  ~AdminRegion();
  friend class apache::geode::statistics::HostStatSampler;
};

typedef SharedPtr<AdminRegion> AdminRegionPtr;
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_ADMINREGION_H_
