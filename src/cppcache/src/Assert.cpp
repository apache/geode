/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/Assert.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/Log.hpp>

namespace gemfire {

void Assert::throwAssertion(const char* expressionText, const char* file,
                            int line) {
  LOGERROR("AssertionException: ( %s ) at %s:%d", expressionText, file, line);

  AssertionException ae(expressionText, NULL, true);
  ae.printStackTrace();
  throw ae;
}
}  // namespace gemfire
