/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __THINCLIENT_CACHE_DISTRIBUTION_MANAGER_HPP__
#define __THINCLIENT_CACHE_DISTRIBUTION_MANAGER_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "ThinClientDistributionManager.hpp"

namespace gemfire {
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

};      // namespace gemfire
#endif  // __THINCLIENT_CACHE_DISTRIBUTION_MANAGER_HPP__
