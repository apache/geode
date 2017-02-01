#pragma once

#ifndef GEODE_THINCLIENTHAREGION_H_
#define GEODE_THINCLIENTHAREGION_H_

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

#include "ThinClientRegion.hpp"
#include <gfcpp/Pool.hpp>

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_THINCLIENTHAREGION_H_
