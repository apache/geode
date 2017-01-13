/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TCR_DISTRIBUTION_MANAGER_HPP__
#define __TCR_DISTRIBUTION_MANAGER_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include "ThinClientDistributionManager.hpp"

namespace gemfire {

class ThinClientRegion;
/**
 * @brief Distribute data between caches
 */
class CPPCACHE_EXPORT TcrDistributionManager
    : public ThinClientDistributionManager {
 public:
  TcrDistributionManager(ThinClientRegion* region,
                         TcrConnectionManager& connManager);

 protected:
  virtual void getEndpointNames(std::unordered_set<std::string>& endpointNames);

  virtual void postUnregisterAction();

  virtual bool preFailoverAction();

  virtual bool postFailoverAction(TcrEndpoint* endpoint);

  virtual void destroyAction();

 private:
  // Disallow copy constructor and assignment operator.
  TcrDistributionManager(const TcrDistributionManager&);
  TcrDistributionManager& operator=(const TcrDistributionManager&);
};
};      // namespace gemfire
#endif  // __TCR_DISTRIBUTION_MANAGER_HPP__
