#pragma once

#ifndef GEODE_CQATTRIBUTESIMPL_H_
#define GEODE_CQATTRIBUTESIMPL_H_

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

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_CQATTRIBUTESIMPL_H_
