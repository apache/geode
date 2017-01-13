#ifndef _GEMFIRE_SPEHELPER_HPP_
#define _GEMFIRE_SPEHELPER_HPP_

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"
#include "TypeHelper.hpp"
#include <typeinfo>

namespace gemfire {

class SharedBase;

/** Helper class for SharedPtr exceptions
*/
class CPPCACHE_EXPORT SPEHelper {
 public:
  static void throwNullPointerException(const char* ename);

  static void throwClassCastException(const char* msg, const char* fromType,
                                      const char* toType);
};

/* Helper function template for type conversion.
 */
template <class Target, class Src>
Target* getTargetHelper(Src* ptr, gemfire::TypeHelper::yes_type yes) {
  return ptr;
}

/* Helper function template for type conversion.
 */
template <class Target, class Src>
Target* getTargetHelper(Src* ptr, gemfire::TypeHelper::no_type no) {
  Target* tptr = dynamic_cast<Target*>(ptr);
  if (tptr) {
    return tptr;
  } else {
    SPEHelper::throwClassCastException("getTargetHelper: cast failed",
                                       typeid(ptr).name(), typeid(tptr).name());
    return NULL;
  }
}

/* Helper function template for type conversion.
 */
template <class Target, class Src>
Target* getTarget(Src* ptr) {
  return getTargetHelper<Target>(ptr, GF_SRC_IS_TARGET_TYPE(Target, Src));
}

/* Helper function template for type conversion.
 */
template <class Src>
SharedBase* getSB(Src* ptr) {
  return getTarget<SharedBase>(ptr);
}
}

#endif
