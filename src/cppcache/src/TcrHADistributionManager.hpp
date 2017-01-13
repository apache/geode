/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TCRHA_DISTRIBUTION_MANAGER_HPP__
#define __TCRHA_DISTRIBUTION_MANAGER_HPP__

#include <gfcpp/gf_base.hpp>
#include "ThinClientDistributionManager.hpp"
#include <gfcpp/CacheAttributes.hpp>
#include "TcrEndpoint.hpp"

namespace gemfire {

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

};  // namespace gemfire

#endif  // __TCRHA_DISTRIBUTION_MANAGER_HPP__
