#pragma once

#ifndef GEODE_GFCPP_SHAREDPTRHELPER_H_
#define GEODE_GFCPP_SHAREDPTRHELPER_H_

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

#include "gfcpp_globals.hpp"
#include "TypeHelper.hpp"
#include <typeinfo>

namespace apache {
namespace geode {
namespace client {

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
Target* getTargetHelper(Src* ptr,
                        apache::geode::client::TypeHelper::yes_type yes) {
  return ptr;
}

/* Helper function template for type conversion.
 */
template <class Target, class Src>
Target* getTargetHelper(Src* ptr,
                        apache::geode::client::TypeHelper::no_type no) {
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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_SHAREDPTRHELPER_H_
