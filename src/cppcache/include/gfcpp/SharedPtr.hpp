#ifndef _GEMFIRE_SHAREDPTR_HPP_
#define _GEMFIRE_SHAREDPTR_HPP_

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "SharedBase.hpp"
#include "Assert.hpp"
#include "TypeHelper.hpp"
#include <typeinfo>
#include "SharedPtrHelper.hpp"

/** @file
*/

namespace gemfire {

#if GF_DEVEL_ASSERTS == 1
#define GF_CHECK_NPE(x) \
  if (x != NULL) {      \
  } else                \
    gemfire::SPEHelper::throwNullPointerException(typeid(*this).name())
#else
#define GF_CHECK_NPE(x)
#endif

class MapEntry;
class MapEntryImpl;

template <class Target>
/** Defines a reference counted shared pointer
*/
class SharedPtr {
 public:
  /** Constructor. */
  inline SharedPtr() : m_ptr(NULL) {}

  /** Constructor for the NULL pointer */
  inline SharedPtr(const NullSharedBase* ptr) : m_ptr(NULL) {}

  /** Explicit copy constructor, given a pointer.
   * @throws ClassCastException if <code>Target</code> pointer cannot be
   * converted to <code>SharedBase</code> pointer (dynamic_cast to
   * <code>SharedBase*</code> fails).
   */
  inline explicit SharedPtr(const Target* ptr)
      : m_ptr(const_cast<Target*>(ptr)) {
    if (NULL != m_ptr) getSB(m_ptr)->preserveSB();
  }

  /** Constructor, given another SharedPtr. */
  inline SharedPtr(const SharedPtr& other) : m_ptr(other.m_ptr) {
    if (NULL != m_ptr) getSB(m_ptr)->preserveSB();
  }

  /** Constructor, given another SharedPtr.
   * @throws ClassCastException if <code>Other</code> pointer cannot be
   * converted to <code>Target</code> pointer (dynamic_cast to
   * <code>Target*</code> fails).
   */
  template <class Other>
  inline SharedPtr(const SharedPtr<Other>& other)
      : m_ptr(getTarget<Target>(other.ptr())) {
    if (NULL != m_ptr) getSB(m_ptr)->preserveSB();
  }

  /** Destructor. */
  inline ~SharedPtr() {
    if (NULL != m_ptr) getSB(m_ptr)->releaseSB();

    m_ptr = NULL;
  }

  inline Target* operator->() const {
    GF_CHECK_NPE(m_ptr);
    GF_DEV_ASSERT(getSB(m_ptr)->refCount() > 0);

    return m_ptr;
  }

  inline Target& operator*() const {
    GF_CHECK_NPE(m_ptr);
    return *m_ptr;
  }

  /** Assigns a pointer.
   * @throws ClassCastException if <code>Target</code> pointer cannot be
   * converted to <code>SharedBase</code> pointer (dynamic_cast to
   * <code>SharedBase*</code> fails).
   */
  inline SharedPtr& operator=(Target* other) {
    if (NULL != other) getSB(other)->preserveSB();

    if (NULL != m_ptr) getSB(m_ptr)->releaseSB();

    m_ptr = other;

    return *this;
  }

  inline SharedPtr& operator=(const SharedPtr& other) {
    Target* otherPtr = other.m_ptr;

    if (NULL != otherPtr) {
      getSB(otherPtr)->preserveSB();
    }
    if (NULL != m_ptr) {
      getSB(m_ptr)->releaseSB();
    }
    m_ptr = otherPtr;

    GF_DEV_ASSERT(otherPtr == other.m_ptr);

    return *this;
  }

  /** Assigns a pointer of type <code>Other</code> from a <code>SharedPtr</code>
   * object.
   * @throws ClassCastException if <code>Other</code> pointer cannot be
   * converted to <code>Target</code> pointer (dynamic_cast to
   * <code>Target*</code> fails).
   */
  template <class Other>
  inline SharedPtr& operator=(const SharedPtr<Other>& other) {
    Other* otherPtr = other.ptr();

    Target* otherTargetPtr = getTarget<Target>(otherPtr);

    if (NULL != otherPtr) {
      getSB(otherPtr)->preserveSB();
    }
    if (NULL != m_ptr) {
      getSB(m_ptr)->releaseSB();
    }
    m_ptr = otherTargetPtr;

    GF_DEV_ASSERT(otherPtr == other.ptr());

    return *this;
  }

  inline SharedPtr& operator=(const NullSharedBase* nullOther) {
    if (m_ptr != NULL) {
      getSB(m_ptr)->releaseSB();
    }
    m_ptr = NULL;
    return *this;
  }

  /** Assigns a pointer of type <code>Other</code>.
   * @throws ClassCastException if <code>Other</code> pointer cannot be
   * converted to <code>Target</code> pointer (dynamic_cast to
   * <code>Target*</code> fails),
   * or if <code>Other</code> pointer cannot be converted to
   * <code>SharedBase</code> pointer (dynamic_cast to <code>SharedBase*</code>
   * fails).
   */
  template <class Other>
  inline SharedPtr& operator=(Other* other) {
    Target* otherTargetPtr = getTarget<Target>(other);

    if (NULL != other) {
      getSB(other)->preserveSB();
    }
    if (NULL != m_ptr) {
      getSB(m_ptr)->releaseSB();
    }
    m_ptr = otherTargetPtr;

    return *this;
  }

  inline bool operator==(const Target* other) const { return m_ptr == other; }

  inline bool operator!=(const Target* other) const { return m_ptr != other; }

  inline bool operator==(const NullSharedBase* nullOther) const {
    return m_ptr == NULL;
  }

  inline bool operator!=(const NullSharedBase* nullOther) const {
    return m_ptr != NULL;
  }

  inline bool operator==(const SharedPtr& other) const {
    return m_ptr == other.m_ptr;
  }

  inline bool operator!=(const SharedPtr& other) const {
    return m_ptr != other.m_ptr;
  }

  template <class Other>
  inline bool operator==(const SharedPtr<Other>& other) {
    return ((const void*)m_ptr) == ((const void*)other.ptr());
  }

  template <class Other>
  inline bool operator!=(const SharedPtr<Other>& other) {
    return !operator==(other);
  }

  inline Target* ptr() const { return m_ptr; }

 private:
  /** this constructor deliberately skips touching m_ptr or anything */
  inline explicit SharedPtr(bool noInit) {}

  Target* m_ptr;

  friend class MapEntry;
  friend class MapEntryImpl;
};

typedef SharedPtr<SharedBase> SharedBasePtr;

/** Statically cast the underlying pointer to the given type. The behaviour
  * is similar to <code>static_cast</code>.
  *
  * Make use of this cast with care since it does not offer protection
  * against incorrect casts. For most purposes <code>dynCast</code> is the
  * better choice and this should be used only where the programmer knows
  * the cast to be safe.
  *
  * Setting the macro <code>GF_DEBUG_ASSERTS</code> enables dynamic checking
  * of the cast throwing an <code>AssertionException</code> if the cast fails.
  */
template <class TargetSP, class Other>
TargetSP staticCast(const SharedPtr<Other>& other) {
  GF_D_ASSERT((other.ptr() == NULL) ||
              (dynamic_cast<GF_UNWRAP_SP(TargetSP)*>(other.ptr()) != NULL));

  return TargetSP(static_cast<GF_UNWRAP_SP(TargetSP)*>(other.ptr()));
}

/** Dynamically cast the underlying pointer to the given type and throw
  * <code>ClassCastException</code> if the cast fails.
  */
template <class TargetSP, class Other>
TargetSP dynCast(const SharedPtr<Other>& other) {
  GF_UNWRAP_SP(TargetSP) * otherPtr;

  if ((other.ptr() == NULL)) {
    return NULLPTR;
  } else if ((otherPtr = dynamic_cast<GF_UNWRAP_SP(TargetSP)*>(other.ptr())) !=
             NULL) {
    return TargetSP(otherPtr);
  } else {
    SPEHelper::throwClassCastException(
        "dynCast: cast failed", typeid(other).name(), typeid(TargetSP).name());
    return NULLPTR;
  }
}

/**
 * Dynamically check if the underlying pointer is of the given SharedPtr type.
 */
template <class TargetSP, class Other>
bool instanceOf(const SharedPtr<Other>& other) {
  return (dynamic_cast<GF_UNWRAP_SP(TargetSP)*>(other.ptr()) != NULL);
}
}

#endif
