#ifndef __GEMFIRE_THINCLIENTHAREGION_H__
#define __GEMFIRE_THINCLIENTHAREGION_H__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *
 * The specification of function behaviors is found in the corresponding .cpp
 *file.
 *
 *========================================================================
 */

#include "ThinClientRegion.hpp"
#include <gfcpp/Pool.hpp>

/**
 * @file
 */

namespace gemfire {

/**
 * @class ThinHAClientRegion ThinHAClientRegion.hpp
 *
 * This class manages the interest list functionalities related with
 * native client regions supporting java HA queues.
 *
 * It inherits from ThinClientRegion and overrides interest list
 * send and invalidate methods.
 *
 */
class CPPCACHE_EXPORT ThinClientHARegion : public ThinClientRegion {
 public:
  /**
   * @brief constructor/destructor
   */
  ThinClientHARegion(const std::string& name, CacheImpl* cache,
                     RegionInternal* rPtr,
                     const RegionAttributesPtr& attributes,
                     const CacheStatisticsPtr& stats, bool shared = false,
                     bool enableNotification = true);

  virtual ~ThinClientHARegion() {
    if (m_poolDM) m_tcrdm = NULL;
  };

  virtual void initTCR();

  bool getProcessedMarker();

  void setProcessedMarker(bool mark = true) { m_processedMarker = mark; }
  void addDisMessToQueue();

 protected:
  virtual GfErrType getNoThrow_FullObject(EventIdPtr eventId,
                                          CacheablePtr& fullObject,
                                          VersionTagPtr& versionTag);

 private:
  RegionAttributesPtr m_attribute;
  volatile bool m_processedMarker;
  void handleMarker();

  bool m_poolDM;

  // Disallow copy constructor and assignment operator.
  ThinClientHARegion(const ThinClientHARegion&);
  ThinClientHARegion& operator=(const ThinClientHARegion&);

  void acquireGlobals(bool isFailover);
  void releaseGlobals(bool isFailover);

  void destroyDM(bool keepEndpoints);
};

};  // namespace gemfire

#endif  // ifndef __GEMFIRE_THINCLIENTHAREGION_H__
