#ifndef _GEMFIRE_ASSERT_HPP_
#define _GEMFIRE_ASSERT_HPP_
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"

/**
 * @file
 *
 *  Assertion functions for debugging
 */

namespace gemfire {

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
}

/** Throws the given assertion. */
#define GF_R_ASSERT(x) gemfire::Assert::assertTrue(x, #x, __FILE__, __LINE__)

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
#define GF_D_ASSERT(x) gemfire::Assert::assertTrue(x, #x, __FILE__, __LINE__)
#else
#define GF_D_ASSERT(x)
#endif

/** Throws the given assertion if GF_DEVEL_ASSERTS is true. */
#if GF_DEVEL_ASSERTS == 1
#define GF_DEV_ASSERT(x) gemfire::Assert::assertTrue(x, #x, __FILE__, __LINE__)
#else
#define GF_DEV_ASSERT(x)
#endif

#endif
