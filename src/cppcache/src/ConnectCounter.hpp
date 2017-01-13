/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_IMPL_CONNECTCOUNTER_HPP_
#define _GEMFIRE_IMPL_CONNECTCOUNTER_HPP_ 1

#include <gfcpp/gfcpp_globals.hpp>

namespace gemfire {

class CPPCACHE_EXPORT ConnectCounter {
 public:
  ConnectCounter();

  virtual ~ConnectCounter();

  virtual void inc(const char* clientName) = 0;
  virtual void dec(const char* clientName) = 0;
};
}

#endif
