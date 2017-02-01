#pragma once

#ifndef GEODE_GFCPP_ASSERT_H_
#define GEODE_GFCPP_ASSERT_H_

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

/**
 * @file
 *
 *  Assertion functions for debugging
 */

namespace apache {
namespace geode {
namespace client {

/**
 * @class Assert Assert.hpp
 *
 * Declares debugging assertion reporting functions.
 */
class CPPCACHE_EXPORT Assert {
 public:
  /** If the given expression is true, does nothing, otherwise calls
  * @ref throwAssertion .
  */
  inline static void assertTrue(bool expression, const char* expressionText,
                                const char* file, int line) {
    if (!expression) {
      throwAssertion(expressionText, file, line);
    }
  }

  /** Throws the given assertion.
  */
  static void throwAssertion(const char* expressionText, const char* file,
                             int line);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

/** Throws the given assertion. */
#define GF_R_ASSERT(x) \
  apache::geode::client::Assert::assertTrue(x, #x, __FILE__, __LINE__)

#ifndef GF_DEBUG_ASSERTS
/** Change this to 1 to use assertion functions. */
#define GF_DEBUG_ASSERTS 0
#endif

#ifndef GF_DEVEL_ASSERTS
#define GF_DEVEL_ASSERTS 0
#endif

#if GF_DEVEL_ASSERTS == 1
#undef GF_DEBUG_ASSERTS
#define GF_DEBUG_ASSERTS 1
#endif

#if GF_DEBUG_ASSERTS == 1
#undef GF_DEVEL_ASSERTS
#define GF_DEVEL_ASSERTS 1
#endif

/** Throws the given assertion if GF_DEBUG_ASSERTS is true. */
#if GF_DEBUG_ASSERTS == 1
#define GF_D_ASSERT(x) \
  apache::geode::client::Assert::assertTrue(x, #x, __FILE__, __LINE__)
#else
#define GF_D_ASSERT(x)
#endif

/** Throws the given assertion if GF_DEVEL_ASSERTS is true. */
#if GF_DEVEL_ASSERTS == 1
#define GF_DEV_ASSERT(x) \
  apache::geode::client::Assert::assertTrue(x, #x, __FILE__, __LINE__)
#else
#define GF_DEV_ASSERT(x)
#endif

#endif // GEODE_GFCPP_ASSERT_H_
