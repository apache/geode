#pragma once

#ifndef GEODE_GFCPP_CQATTRIBUTES_H_
#define GEODE_GFCPP_CQATTRIBUTES_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "VectorT.hpp"

#include "CqListener.hpp"
/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

/**
 * @cacheserver
 * Querying is only supported for native clients.
 * @endcacheserver
 * @class CqAttributes CqAttributes.hpp
 *
 * This interface holds all attribute values for a CQ and provides methods for
 * retrieving all attribute settings. This interface can be modified only by
 * the CqAttributesFactory class (before CQ creation) and the
 * CqAttributesMutator
 * interface (after CQ creation).
 *
 * For compatibility rules and default values, see {@link CqAttributesFactory}.
 */
class CPPCACHE_EXPORT CqAttributes : virtual public SharedBase {
 public:
  /**
   * Get the CqListeners set with the CQ.
   * Returns all the Listeners associated with this CQ.
   * @see CqListener
   * @return VectorOfCqListener of CqListnerPtr
   */
  virtual void getCqListeners(VectorOfCqListener& vl) = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CQATTRIBUTES_H_
