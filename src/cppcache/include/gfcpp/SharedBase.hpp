#ifndef _GEMFIRE_SHAREDBASE_HPP_
#define _GEMFIRE_SHAREDBASE_HPP_

// SharedBase.hpp     -*- mode: c++ -*-

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"

/** @file
*/

namespace gemfire {

/**
 * @class SharedBase SharedBase.hpp
 *
 * This abstract base class is the base class of all user objects
 * that have the shared capability of reference counting.
 */
class CPPCACHE_EXPORT SharedBase {
 public:
  /** Constructor. */
  inline SharedBase() : m_refCount(0) {}

  /** Atomically increment reference count */
  void preserveSB() const;

  /**
   * Atomically decrement reference count, the SharedBase object is
   * automatically deleted when its reference count goes to zero
   */
  void releaseSB() const;

  /** @return the reference count */
  inline int32_t refCount() { return m_refCount; }

 protected:
  inline SharedBase(bool noInit) {}

  virtual ~SharedBase() {}

 private:
  mutable volatile int32_t m_refCount;

  void operator=(const SharedBase& rhs);
};

/**
 * Class encapsulating a NULL SharedBase smart pointer. This is for passing
 * NULL pointers implicitly to copy constructor of <code>SharedPtr</code>
 * class.
 */
class CPPCACHE_EXPORT NullSharedBase : public SharedBase {
 public:
  static const NullSharedBase* const s_instancePtr;

 private:
  NullSharedBase() {}
  // never defined
  NullSharedBase(const NullSharedBase&);
  NullSharedBase& operator=(const NullSharedBase&);

  friend class SharedBase;  // just to get rid of warning with gcc3.x
};
}

#define NULLPTR gemfire::NullSharedBase::s_instancePtr

#endif  //#define _GEMFIRE_SHAREDBASE_HPP_
