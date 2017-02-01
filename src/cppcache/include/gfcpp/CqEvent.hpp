#pragma once

#ifndef GEODE_GFCPP_CQEVENT_H_
#define GEODE_GFCPP_CQEVENT_H_

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
#include "Exception.hpp"
#include "CqOperation.hpp"
#include "Cacheable.hpp"
#include "CacheableKey.hpp"
#include "CacheableBuiltins.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

/**
 * @class CqEvent CqEvent.hpp
 *
 * This interface provides methods to get all the information sent from the
 * server
 * about the CQ event.
 * The CqEvent is passed to the CQs CqListener methods. It can be used to
 * retrieve
 * such information as the region operation, CQ operation associated with the
 * event,
 * the new key and value from the event, and the CqQuery object associated with
 * the
 * event.
 * The CqEvent is not an extension of CacheEvent.
 */
class CPPCACHE_EXPORT CqEvent {
 public:
  CqEvent() {}

  virtual ~CqEvent() {}
  /**
   * Get the CqQuery object of this event.
   * @see CqQuery
   * @return CqQuery object.
   */
  virtual CqQueryPtr getCq() const = 0;

  /**
   * Get the operation on the base region that triggered this event.
   * @return Operation operation on the base region (on which CQ is created).
   */
  virtual CqOperation::CqOperationType getBaseOperation() const = 0;

  /**
   * Get the operation on the query results. Supported operations
   * include update, create, destroy, region clear and region invalidate.
   * @return Operation operation with respect to CQ.
   */
  virtual CqOperation::CqOperationType getQueryOperation() const = 0;

  /**
   * Get the key relating to the event.
   * In case of REGION_CLEAR and REGION_INVALIDATE operation, the key will be
   * NULLPTR.
   * @return Object key.
   */
  virtual CacheableKeyPtr getKey() const = 0;

  /**
   * Get the new value of the modification.
   * If there is no new value returns NULLPTR, this will happen during delete
   * operation.
   * @return Object new/modified value.
   */
  virtual CacheablePtr getNewValue() const = 0;

  /**
  * Get the delta modification.
  * If there is no delta, returns null. New value may still be available.
  *
  * @return CacheableBytes delta value.
  */
  virtual CacheableBytesPtr getDeltaValue() const = 0;

 private:
  CqEvent(const CqEvent&);
  void operator=(const CqEvent&);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CQEVENT_H_
