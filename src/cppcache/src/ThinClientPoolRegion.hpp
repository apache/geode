/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientPoolRegion.hpp
 *
 *  Created on: Nov 20, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTPOOLREGION_HPP_
#define THINCLIENTPOOLREGION_HPP_

#include "ThinClientHARegion.hpp"

namespace gemfire {
class ThinClientPoolRegion : public ThinClientRegion {
 public:
  /**
   * @brief constructor/initializer/destructor
   */
  ThinClientPoolRegion(const std::string& name, CacheImpl* cache,
                       RegionInternal* rPtr,
                       const RegionAttributesPtr& attributes,
                       const CacheStatisticsPtr& stats, bool shared = false);

  virtual void initTCR();
  virtual ~ThinClientPoolRegion();

 private:
  virtual void destroyDM(bool keepEndpoints);

  // Disallow copy constructor and assignment operator.
  ThinClientPoolRegion(const ThinClientPoolRegion&);
  ThinClientPoolRegion& operator=(const ThinClientPoolRegion&);
};
}

#endif /* THINCLIENTPOOLREGION_HPP_ */
