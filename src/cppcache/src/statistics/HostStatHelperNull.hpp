#ifndef _GEMFIRE_STATISTICS_HOSTSTATHELPERNULL_HPP_
#define _GEMFIRE_STATISTICS_HOSTSTATHELPERNULL_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

/** @file
 */

namespace gemfire_statistics {

class HostStatHelperNull {
 public:
  static void refreshProcess(ProcessStats* processStats) {}
};
}

#endif  // _GEMFIRE_STATISTICS_HOSTSTATHELPERLINUX_HPP_
