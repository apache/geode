#ifndef __GEMFIRE_CQ_ATTRIBUTES_MUTATOR_H__
#define __GEMFIRE_CQ_ATTRIBUTES_MUTATOR_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "VectorT.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class CqAttributesMutator CqAttributesMutator.hpp
 *
 * This interface is used to modify the listeners that are associated with a CQ.
 * Each CqQuery has an CqAttributesMutator interface which supports modification
 * of certain CQ attributes after the CQ has been created.
 *
 */
class CPPCACHE_EXPORT CqAttributesMutator : virtual public SharedBase {
 public:
  /**
   * Adds a CQ listener to the end of the list of CQ listeners on this CqQuery.
   * @param aListener the user defined CQ listener to add to the CqQuery.
   * @throws IllegalArgumentException if <code>aListener</code> is NULLPTR
   */
  virtual void addCqListener(const CqListenerPtr& aListener) = 0;

  /**
   * Removes given CQ listener from the list of CQ listeners on this CqQuery.
   * Does nothing if the specified listener has not been added.
   * If the specified listener has been added then will
   * be called on it; otherwise does nothing.
   * @param aListener the CQ listener to remove from the CqQuery.
   * @throws IllegalArgumentException if <code>aListener</code> is NULLPTR
   */
  virtual void removeCqListener(const CqListenerPtr& aListener) = 0;

  /**
   * Adds the given set CqListner on this CQ. If the CQ already has CqListeners,
   * this
   * removes those old CQs and initializes with the newListeners.
   * @param newListeners a possibly empty array of listeners to add
   * to this CqQuery.
   * @throws IllegalArgumentException if the <code>newListeners</code> array
   * has a NULLPTR element
   */
  virtual void setCqListeners(VectorOfCqListener& newListeners) = 0;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_ATTRIBUTES_MUTATOR_H__
