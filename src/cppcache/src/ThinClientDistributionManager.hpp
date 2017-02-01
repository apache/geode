#pragma once

#ifndef GEODE_THINCLIENTDISTRIBUTIONMANAGER_H_
#define GEODE_THINCLIENTDISTRIBUTIONMANAGER_H_

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

#include "ThinClientBaseDM.hpp"

namespace apache {
namespace geode {
namespace client {
class ThinClientDistributionManager : public ThinClientBaseDM {
 public:
  ThinClientDistributionManager(TcrConnectionManager& connManager,
                                ThinClientRegion* region);
  virtual ~ThinClientDistributionManager() {}

  virtual void init();
  virtual void destroy(bool keepalive = false);

  virtual GfErrType sendSyncRequest(TcrMessage& request, TcrMessageReply& reply,
                                    bool attemptFailover = true,
                                    bool isBGThread = false);

  void failover();

  void acquireFailoverLock() { m_endpointsLock.acquire_read(); };
  void releaseFailoverLock() { m_endpointsLock.release(); };

  TcrEndpoint* getActiveEndpoint() { return m_endpoints[m_activeEndpoint]; }
  bool isEndpointAttached(TcrEndpoint* ep);

  GfErrType sendRequestToEP(const TcrMessage& request, TcrMessageReply& reply,
                            TcrEndpoint* ep);

 protected:
  virtual void getEndpointNames(std::unordered_set<std::string>& endpointNames);

  GfErrType selectEndpoint(std::vector<int>& randIndex, bool& doRand,
                           bool useActiveEndpoint = false,
                           bool forceSelect = false);

  GfErrType connectToEndpoint(int epIndex);

  virtual void postUnregisterAction();

  virtual bool preFailoverAction();

  virtual bool postFailoverAction(TcrEndpoint* endpoint);

  virtual void destroyAction();

  PropertiesPtr getCredentials(TcrEndpoint* ep);

  GfErrType sendUserCredentials(PropertiesPtr credentials, TcrEndpoint* ep);

  volatile int m_activeEndpoint;

  std::vector<TcrEndpoint*> m_endpoints;
  ACE_Recursive_Thread_Mutex m_endpointsLock;
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_THINCLIENTDISTRIBUTIONMANAGER_H_
