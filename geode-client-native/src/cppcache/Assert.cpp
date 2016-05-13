/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "Assert.hpp"
#include "ExceptionTypes.hpp"
#include "Log.hpp"
#include "SignalHandler.hpp"

namespace gemfire {

void Assert::throwAssertion( const char* expressionText, const char* file, int line ) 
{
  LOGERROR( "AssertionException: ( %s ) at %s:%d", expressionText, file, line );

  AssertionException ae( expressionText, NULL, true );
  ae.printStackTrace();
#ifdef DEBUG
  gemfire::SignalHandler::waitForDebugger();
#endif
  throw ae;
}

}


