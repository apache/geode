
#ifndef __GEMFIRE_INTERESTRESULTPOLICY_H__
#define __GEMFIRE_INTERESTRESULTPOLICY_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */
#include <gfcpp/gfcpp_globals.hpp>

namespace gemfire {
/**
 * @class InterestResultPolicy InterestResultPolicy.hpp
 * Policy class for interest result.
 */
class CPPCACHE_EXPORT InterestResultPolicy {
  // public static methods
 public:
  static char nextOrdinal;

  static InterestResultPolicy NONE;
  static InterestResultPolicy KEYS;
  static InterestResultPolicy KEYS_VALUES;

  char ordinal;

  char getOrdinal() { return ordinal; }

 private:
  InterestResultPolicy() { ordinal = nextOrdinal++; }
};

}  // namespace gemfire
#endif  // ifndef __GEMFIRE_INTERESTRESULTPOLICY_H__
