#pragma once

#ifndef GEODE_TCRHADISTRIBUTIONMANAGER_H_
#define GEODE_TCRHADISTRIBUTIONMANAGER_H_

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

#include <gfcpp/gf_base.hpp>
#include "ThinClientDistributionManager.hpp"
#include <gfcpp/CacheAttributes.hpp>
#include "TcrEndpoint.hpp"

namespace apache {
namespace geode {
namespace client {

class ThinClientRegion;
class ThinClientHARegion;
/**
 * @brief Distribute data between caches
 */
class CPPCACHE_EXPORT TcrHADistributionManager
    : public ThinClientDistributionManager {
 public:
  TcrHADistributionManager(ThinClientRegion* theRegion,
                           TcrConnectionManager& connManager,
                           CacheAttributesPtr cacheAttributes);

  void init();

  GfErrType registerInterestForRegion(TcrEndpoint* ep,
                                      const TcrMessage* request,
                                      TcrMessageReply* reply);

  GfErrType sendSyncRequestRegisterInterestEP(TcrMessage& request,
                                              TcrMessageReply& reply,
                                              bool attemptFailover,
                                              TcrEndpoint* endpoint);

  virtual GfErrType sendRequestToEP(const TcrMessage& request,
                                    TcrMessageReply& reply,
                                    TcrEndpoint* endpoint);

  ThinClientRegion* getRegion() { return m_region; }

  virtual void acquireRedundancyLock() {
    m_connManager.acquireRedundancyLock();
  };
  virtual void releaseRedundancyLock() {
    m_connManager.releaseRedundancyLock();
  };

 protected:
  virtual GfErrType sendSyncRequestRegisterInterest(
      TcrMessage& request, TcrMessageReply& reply, bool attemptFailover = true,
      ThinClientRegion* region = NULL, TcrEndpoint* endpoint = NULL);

  virtual GfErrType sendSyncRequestCq(TcrMessage& request,
                                      TcrMessageReply& reply);

  virtual void getEndpointNames(std::unordered_set<std::string>& endpointNames);

  virtual bool preFailoverAction();

  virtual bool postFailoverAction(TcrEndpoint* endpoint);

 private:
  // Disallow copy constructor and assignment operator.
  TcrHADistributionManager(const TcrHADistributionManager&);
  TcrHADistributionManager& operator=(const TcrHADistributionManager&);
  CacheAttributesPtr m_cacheAttributes;
  TcrConnectionManager& m_theTcrConnManager;

  GfErrType sendRequestToPrimary(TcrMessage& request, TcrMessageReply& reply) {
    return m_theTcrConnManager.sendRequestToPrimary(request, reply);
  }

  friend class ThinClientHARegion;
  friend class TcrConnectionManager;
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_TCRHADISTRIBUTIONMANAGER_H_
