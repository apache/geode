#pragma once

#ifndef GEODE_THINCLIENTPOOLREGION_H_
#define GEODE_THINCLIENTPOOLREGION_H_

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
/*
 * ThinClientPoolRegion.hpp
 *
 *  Created on: Nov 20, 2008
 *      Author: abhaware
 */


#include "ThinClientHARegion.hpp"

namespace apache {
namespace geode {
namespace client {
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
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_THINCLIENTPOOLREGION_H_
