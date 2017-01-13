#ifndef __GEMFIRE_CQ_ATTRIBUTES_H__
#define __GEMFIRE_CQ_ATTRIBUTES_H__
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

#include "CqListener.hpp"
/**
 * @file
 */

namespace gemfire {

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

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_ATTRIBUTES_H__
