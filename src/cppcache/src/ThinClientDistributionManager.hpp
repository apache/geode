/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __THINCLIENT_DISTRIBUTION_MANAGER_HPP__
#define __THINCLIENT_DISTRIBUTION_MANAGER_HPP__

#include "ThinClientBaseDM.hpp"

namespace gemfire {
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

}  // namespace gemfire

#endif  // __THINCLIENT_DISTRIBUTION_MANAGER_HPP__
