#ifndef __GEMFIRE_PDXUNREADFIELDS_HPP_
#define __GEMFIRE_PDXUNREADFIELDS_HPP_

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "gf_base.hpp"
#include "SharedBase.hpp"

namespace gemfire {

/**
 * Marker interface for an object that GemFire creates and returns
 * from {@link PdxReader#readUnreadFields() readUnreadFields}.
 * If you call readUnreadFields then you must also call
 * {@link PdxWriter#writeUnreadFields(PdxUnreadFields) writeUnreadFields} when
 * that object is reserialized. If you do not call {@link
 *PdxWriter#writeUnreadFields(PdxUnreadFields) writeUnreadFields}
 * but you did call {@link PdxReader#readUnreadFields() readUnreadFields} the
 *unread fields will not be written.
 * <p>Unread fields are those that are not explicitly read with a {@link
 *PdxReader} readXXX method.
 * This should only happen when a domain class has changed by adding or removing
 *one or more fields.
 **/
class CPPCACHE_EXPORT PdxUnreadFields : public SharedBase {
 public:
  PdxUnreadFields() {}
  virtual ~PdxUnreadFields() {}
};
}

#endif /* __GEMFIRE_PDXUNREADFIELDS_HPP_ */
