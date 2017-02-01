#pragma once

#ifndef GEODE_CQEVENTIMPL_H_
#define GEODE_CQEVENTIMPL_H_

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

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/CqEvent.hpp>
#include <gfcpp/CqOperation.hpp>
#include <gfcpp/CqQuery.hpp>
#include <gfcpp/CacheableKey.hpp>
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/Exception.hpp>
#include <string>

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
 * @class CqEventImpl EventImpl.hpp
 *
 * Interface for CqEvent. Offers methods to get information from
 * CqEvent.
 */

class ThinClientBaseDM;

class CqEventImpl : public CqEvent {
 public:
  CqEventImpl(CqQueryPtr& cQuery, CqOperation::CqOperationType baseOp,
              CqOperation::CqOperationType cqOp, CacheableKeyPtr& key,
              CacheablePtr& value, ThinClientBaseDM* tcrdm,
              CacheableBytesPtr deltaBytes, EventIdPtr eventId);

  CqQueryPtr getCq() const;

  /**
   * Get the operation on the base region that triggered this event.
   */
  CqOperation::CqOperationType getBaseOperation() const;

  /**
   * Get the the operation on the query results. Supported operations include
   * update, create, and destroy.
   */
  CqOperation::CqOperationType getQueryOperation() const;

  /**
   * Get the key relating to the event.
   * @return Object key.
   */
  CacheableKeyPtr getKey() const;

  /**
   * Get the new value of the modification.
   *  If there is no new value because this is a delete, then
   *  return null.
   */
  CacheablePtr getNewValue() const;

  bool getError();

  std::string toString();

  CacheableBytesPtr getDeltaValue() const;

 private:
  CqEventImpl();
  CqQueryPtr m_cQuery;
  CqOperation::CqOperationType m_baseOp;
  CqOperation::CqOperationType m_queryOp;
  CacheableKeyPtr m_key;
  CacheablePtr m_newValue;
  bool m_error;
  ThinClientBaseDM* m_tcrdm;
  CacheableBytesPtr m_deltaValue;
  EventIdPtr m_eventId;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_CQEVENTIMPL_H_
