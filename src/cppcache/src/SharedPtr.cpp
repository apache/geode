/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/SharedPtr.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <Utils.hpp>

#include <string>

using namespace gemfire;

void SPEHelper::throwNullPointerException(const char* ptrtype) {
  throw NullPointerException(Utils::demangleTypeName(ptrtype)->asChar(), NULL,
                             true);
}

void SPEHelper::throwClassCastException(const char* msg, const char* fromType,
                                        const char* toType) {
  std::string exMsg(msg);
  exMsg +=
      ((std::string) " from '" + Utils::demangleTypeName(fromType)->asChar() +
       "' to '" + Utils::demangleTypeName(toType)->asChar() + "'.");
  throw ClassCastException(exMsg.c_str());
}
