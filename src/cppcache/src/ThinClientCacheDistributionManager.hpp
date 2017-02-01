#pragma once

#ifndef GEODE_THINCLIENTCACHEDISTRIBUTIONMANAGER_H_
#define GEODE_THINCLIENTCACHEDISTRIBUTIONMANAGER_H_

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

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "ThinClientDistributionManager.hpp"

namespace apache {
namespace geode {
namespace client {
/**
 * @brief Distribute data between caches
 */
class TcrMessage;
class TcrConnection;

class CPPCACHE_EXPORT ThinClientCacheDistributionManager
    : public ThinClientDistributionManager,
      public SharedBase {
 public:
  ThinClientCacheDistributionManager(TcrConnectionManager& connManager);
  ~ThinClientCacheDistributionManager(){};

  void init();
  virtual GfErrType sendSyncRequest(TcrMessage& request, TcrMessageReply& reply,
                                    bool attemptFailover = true,
                                    bool isBGThread = false);

  GfErrType sendSyncRequestCq(TcrMessage& request, TcrMessageReply& reply);
  GfErrType sendRequestToPrimary(TcrMessage& request, TcrMessageReply& reply);

 protected:
  bool preFailoverAction();
  bool postFailoverAction(TcrEndpoint* endpoint);

 private:
  // Disallow default/copy constructor and assignment operator.
  ThinClientCacheDistributionManager();
  ThinClientCacheDistributionManager(const ThinClientCacheDistributionManager&);
  ThinClientCacheDistributionManager& operator=(
      const ThinClientCacheDistributionManager&);
};

typedef SharedPtr<ThinClientCacheDistributionManager>
    ThinClientCacheDistributionManagerPtr;
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_THINCLIENTCACHEDISTRIBUTIONMANAGER_H_
