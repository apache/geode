#ifndef __GEMFIRE_CQ_EVENT_IMPL_H__
#define __GEMFIRE_CQ_EVENT_IMPL_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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

namespace gemfire {

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

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_EVENT_IMPL_H__
