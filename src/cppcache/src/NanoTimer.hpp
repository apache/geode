/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GEMFIRE_IMPL_NANOTIMER_HPP_
#define _GEMFIRE_IMPL_NANOTIMER_HPP_

#include <gfcpp/gfcpp_globals.hpp>

namespace gemfire {

class CPPCACHE_EXPORT NanoTimer {
 public:
  static int64_t now();

  static void sleep(uint32_t nanos);
};
}

#endif
