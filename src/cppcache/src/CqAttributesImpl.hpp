#ifndef __GEMFIRE_CQ_ATTRIBUTES_IMPL_H__
#define __GEMFIRE_CQ_ATTRIBUTES_IMPL_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/CqAttributes.hpp>
#include <gfcpp/CqAttributesMutator.hpp>
#include <ace/ACE.h>
#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>
#include <ace/Recursive_Thread_Mutex.h>

/**
 * @file
 */

namespace gemfire {

/**
 * @class CqAttributesImpl CqAttributesImpl.hpp
 *
 * This interface holds all attribute values for a CQ and provides methods for
 * retrieving all attribute settings. This interface can be modified only by
 * the CqAttributesFactory class (before CQ creation) and the
 * CqAttributesMutator
 * interface (after CQ creation).
 *
 * For compatibility rules and default values, see {@link CqAttributesFactory}.
 */
class CPPCACHE_EXPORT CqAttributesImpl : public CqAttributes {
 public:
  /**
   * Get the CqListeners set with the CQ.
   * Returns all the Listener associated with this CQ.
   * @see CqListener
   * @return VectorOfCqListener of CqListnerPtr
   */
  void getCqListeners(VectorOfCqListener& vl);

  /**
   * Get the CqListener set with the CQ.
   * Returns the CqListener associated with the CQ.
   * If there are more than one CqListener throws IllegalStateException.
   * @see CqListener
   * @return CqListener Object, returns null if there is no CqListener.
   */
  CqListenerPtr getCqListener();
  void addCqListener(CqListenerPtr& cql);
  void setCqListeners(VectorOfCqListener& addedListeners);
  void removeCqListener(CqListenerPtr& cql);
  CqAttributesImpl* clone();

 private:
  VectorOfCqListener m_cqListeners;
  bool m_dataPolicyHasBeenSet;
  ACE_Recursive_Thread_Mutex m_mutex;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_ATTRIBUTES_IMPL_H__
